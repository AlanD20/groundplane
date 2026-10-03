package hierarchydeletionplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionattach"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
	"strings"
)

func (repository *Planner) freezeEnvironmentAttachMembership(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone, environmentID string, prerequisites []string,
) ([]HierarchyDeletionMembershipNode, error) {
	targets, err := repository.hierarchyDeletionIndexedTargets(ctx, operation.SnapshotRevision,
		attachrecord.AttachOwnerPrefix(environmentID), attachrecord.AttachKey, ids.KindAttach,
		validateHierarchyDeletionAttachOwner, environmentID)
	if err != nil {
		return nil, err
	}
	inputs := make(map[string]hierarchydeletionattach.Input, len(targets))
	defer func() {
		for _, input := range inputs {
			hierarchydeletionattach.Clear(&input)
		}
	}()
	for _, target := range targets {
		input, err := hierarchydeletionattach.ReadCaptured(
			ctx,
			repository.store,
			operation.SnapshotRevision,
			operation.OperationID,
			target.id,
		)
		if err != nil {
			return nil, err
		}
		if input.AttachRevision != target.revision {
			hierarchydeletionattach.Clear(&input)
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		if err := hierarchydeletionattach.RequireTerminal(input); err != nil {
			hierarchydeletionattach.Clear(&input)
			return nil, err
		}
		if _, err := attachrecord.RequireAttachBackupSourceExclusionAbsent(ctx, repository.store, target.id, operation.SnapshotRevision); err != nil {
			hierarchydeletionattach.Clear(&input)
			return nil, err
		}
		inputs[target.id] = input
	}
	dependencies := make(map[string][]string, len(targets))
	for _, target := range targets {
		input := inputs[target.id]
		if err := repository.requireAttachOutgoingIndexes(ctx, operation.SnapshotRevision, input.Attach); err != nil {
			return nil, err
		}
		for _, prefix := range []string{attachrecord.AttachGrantedByPrefix(target.id), attachrecord.AttachCredentialByPrefix(target.id)} {
			incoming, err := repository.readAttachIncoming(ctx, operation.SnapshotRevision, prefix, inputs)
			if err != nil {
				return nil, err
			}
			for _, sourceID := range incoming {
				source := inputs[sourceID].Attach
				valid := prefix == attachrecord.AttachGrantedByPrefix(target.id) &&
					slices.Contains(source.GrantAttachIDs, target.id)
				valid = valid ||
					prefix == attachrecord.AttachCredentialByPrefix(target.id) && !source.OwnsCredential() &&
						source.CredentialAttachID == target.id
				if !valid || source.EnvironmentID != environmentID ||
					source.BackingServiceID != input.Attach.BackingServiceID {
					return nil, hierarchydeletion.CorruptHierarchyDeletion()
				}
				dependencies[target.id] = append(dependencies[target.id], "attach:"+sourceID+":finalize")
			}
		}
		for _, grantID := range input.Attach.GrantAttachIDs {
			grant, ok := inputs[grantID]
			if !ok || grant.Attach.BackingServiceID != input.Attach.BackingServiceID || !grant.Attach.OwnsCredential() {
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			if !slices.Contains(dependencies[grantID], "attach:"+target.id+":finalize") {
				// The reverse-index comparison is checked after every target has been read.
				dependencies[grantID] = append(dependencies[grantID], "attach:"+target.id+":finalize")
			}
		}
		if !input.Attach.OwnsCredential() {
			owner, ok := inputs[input.Attach.CredentialAttachID]
			if !ok || !owner.Attach.OwnsCredential() || owner.Attach.BackingServiceID != input.Attach.BackingServiceID {
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
		}
	}
	nodes := make([]HierarchyDeletionMembershipNode, 0, len(targets)*2)
	for _, target := range targets {
		input := inputs[target.id]
		if err := repository.persistAttachInput(ctx, operation, input); err != nil {
			return nil, err
		}
		required := append(append([]string(nil), prerequisites...), dependencies[target.id]...)
		slices.Sort(required)
		required = slices.Compact(required)
		if hierarchydeletionattach.NeedsAgent(input) {
			node := hierarchyDeletionAgentNode("attach:"+target.id+":deprovision", "attach", target.id,
				hierarchydeletion.HierarchyDeletionAttachDeprovision, target.revision, required, operation.OperationID)
			digest, err := hierarchydeletionattach.Digest(input)
			if err != nil {
				return nil, err
			}
			node.fixedInputDigest = digest
			node.ProcedureInput.AgentChild.InputDigest = digest
			nodes = append(nodes, node)
			required = []string{node.NodeID}
		}
		nodes = append(nodes, hierarchyDeletionControllerNode(
			"attach:"+target.id+":finalize",
			"attach",
			target.id,
			hierarchydeletion.HierarchyDeletionAttachFinalize,
			target.revision,
			required,
			"attach.finalize",
			target.digest,
		))
	}
	return nodes, nil
}

func (repository *Planner) requireAttachOutgoingIndexes(
	ctx context.Context,
	revision int64,
	record attachrecord.Record,
) error {
	keys := make([]string, 0, len(record.GrantAttachIDs)+2)
	for _, grantID := range record.GrantAttachIDs {
		keys = append(keys, attachrecord.AttachGrantedByKey(grantID, record.ID))
	}
	if !record.OwnsCredential() {
		keys = append(keys, attachrecord.AttachCredentialByKey(record.CredentialAttachID, record.ID))
	}
	indexCount := len(keys)
	keys = append(keys, attachrecord.AttachDependentGrantKey(record.ID))
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	for index := 0; index < indexCount; index++ {
		if read.Values[index] == nil || read.Values[index].Key != keys[index] ||
			string(read.Values[index].Value) != record.ID {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	grants := read.Values[indexCount]
	if len(record.GrantAttachIDs) == 0 {
		if grants != nil {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		return nil
	}
	if grants == nil {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return attachrecord.ValidateAttachDependentGrantRange(
		&keyvalue.RangeResult{ReadRevision: revision, Values: []keyvalue.KeyValue{*grants}},
		record.ID,
		record.GrantAttachIDs,
	)
}

func (repository *Planner) readAttachIncoming(ctx context.Context, revision int64, prefix string,
	inputs map[string]hierarchydeletionattach.Input,
) ([]string, error) {
	var result []string
	start := ""
	for {
		page, err := repository.store.Range(
			ctx,
			keyvalue.RangeRequest{Prefix: prefix, StartExclusive: start, Limit: 128, Revision: revision},
		)
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 128 {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for _, value := range page.Values {
			id := strings.TrimPrefix(value.Key, prefix)
			if ids.Validate(ids.KindAttach, id) != nil || string(value.Value) != id {
				keyvalue.ClearRangeValues(page.Values)
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			if _, ok := inputs[id]; !ok {
				keyvalue.ClearRangeValues(page.Values)
				return nil, errs.New(errs.KindResourceInUse, "Attach has references outside the deleting Environment")
			}
			result = append(result, id)
			start = value.Key
		}
		more := page.More
		keyvalue.ClearRangeValues(page.Values)
		if !more {
			return result, nil
		}
		if start == "" {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
}

func (repository *Planner) persistAttachInput(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	input hierarchydeletionattach.Input,
) error {
	key := hierarchydeletionattach.FrozenKey(operation.OperationID, input.Attach.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key,
		hierarchydeletion.HierarchyDeletionTombstoneKey(string(operation.TargetKind), operation.TargetID)}})
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 2 || read.Values[1] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	value, err := hierarchydeletionattach.Encode(input)
	if err != nil {
		return err
	}
	defer clear(value)
	if read.Values[0] != nil {
		if string(read.Values[0].Value) != string(value) {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		return nil
	}
	var current hierarchydeletion.HierarchyDeletionTombstone
	if hierarchydeletion.DecodeHierarchyDeletionRecord(
		read.Values[1].Value,
		hierarchydeletion.HierarchyDeletionLargeRecordBytes,
		&current,
	) != nil ||
		current.OperationID != operation.OperationID ||
		current.SnapshotRevision != input.SnapshotRevision ||
		current.Phase != hierarchydeletion.HierarchyDeletionPlanning {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	tx, err := repository.store.Transact(
		ctx,
		[]keyvalue.Condition{{Key: key}, {Key: read.Values[1].Key, ModRevision: read.Values[1].ModRevision}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return err
	}
	keyvalue.ClearValues(tx.FailureReads)
	if !tx.Succeeded {
		return errs.New(errs.KindStateConflict, "hierarchy Attach input publication changed")
	}
	return nil
}
