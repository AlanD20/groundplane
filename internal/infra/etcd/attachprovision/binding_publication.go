package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PreparePendingBinding joins executable-input readiness, not a hidden Task
// publication. No host effects can begin without this exact pending binding.
func (repository *InputRepository) PreparePendingBinding(
	ctx context.Context,
	work workqueue.View,
) (workqueue.Change, error) {
	source, err := repository.LoadWaitingCapture(ctx, work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	generation := source.Generation.Record
	if generation.SettlesInController() {
		return workqueue.Change{}, checkpointConflict(
			"Controller-only Attach does not require pending host provisioning",
		)
	}
	record, err := generation.PendingRecord(work.Task.Task.ID, work.Task.Task.CreatedAt)
	if err != nil {
		return workqueue.Change{}, err
	}
	return repository.publishBinding(ctx, work, generation, record, source.Conditions)
}

func (repository *InputRepository) publishBinding(ctx context.Context, work workqueue.View,
	generation blueprintattachinputs.Generation, record attachments.Record, conditions []keyvalue.Condition,
) (_ workqueue.Change, resultErr error) {
	change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), conditions...)}
	defer func() {
		if resultErr != nil {
			keyvalue.ZeroMutationBytes(change.Mutations)
		}
	}()
	value, err := attachments.EncodeAttachRecord(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: attachments.AttachKey(record.ID)},
		keyvalue.Condition{Key: deletions.TombstoneKey("attach", record.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: attachments.AttachKey(record.ID), Value: value},
	)
	indexes := []string{attachments.AttachNameKey(record.EnvironmentID, record.Name),
		attachments.AttachOwnerKey(
			record.EnvironmentID,
			record.ID,
		), attachments.AttachServiceKey(record.ServiceID, record.ID),
		attachments.AttachBackingServiceKey(
			record.BackingServiceID,
			record.ID,
		), attachments.AttachBackingProjectKey(record.BackingProjectID, record.ID)}
	for _, key := range indexes {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(record.ID)},
		)
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: attachments.AttachFactsKey(record.ID)})
	if record.OwnsCredential() && (len(generation.FactSets) != 0 || record.HookBundle) {
		facts, err := attachments.EncodeAttachEncryptedFacts(generation.GeneratedInputs)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: attachments.AttachFactsKey(record.ID), Value: facts},
		)
	}
	credentialGuards, err := repository.CredentialConditions(ctx, generation, work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, credentialGuards...)
	if !record.OwnsCredential() {
		key := attachments.AttachCredentialByKey(record.CredentialAttachID, record.ID)
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(record.ID)},
		)
	}
	if generation.Provision != nil {
		for _, grant := range generation.Provision.Grants {
			guard, err := repository.grantAdmission(ctx, generation, grant, work.ReadRevision)
			if err != nil {
				return workqueue.Change{}, err
			}
			change.Conditions = append(change.Conditions, guard...)
		}
	}
	for _, grantID := range record.GrantAttachIDs {
		key := attachments.AttachGrantedByKey(grantID, record.ID)
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: []byte(record.ID)},
		)
	}
	if len(record.GrantAttachIDs) > 0 {
		value, err := attachments.EncodeAttachDependentGrantIndex(record.ID, record.GrantAttachIDs)
		if err != nil {
			return workqueue.Change{}, err
		}
		key := attachments.AttachDependentGrantKey(record.ID)
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value},
		)
	}
	for _, environmentID := range []string{record.EnvironmentID, record.BackingEnvironmentID} {
		fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	}
	change.Conditions, change.Mutations = attachments.BindBackingZoneCreation(
		record.BackingNetworkID,
		change.Conditions,
		change.Mutations,
	)
	return change, nil
}

// PrepareDirectBinding joins initial direct Task publication, rather than
// waiting for a Blueprint's input-preparation pass. Controller-only connections
// are pending too until their visible work item settles.
func (repository *InputRepository) PrepareDirectBinding(ctx context.Context, task workexecution.Task,
	item workexecution.Item, generation blueprintattachinputs.Generation, revision int64,
) (workqueue.Change, error) {
	if revision <= 0 || task.Action != workexecution.ActionAttach || task.PublicationComplete ||
		!generation.MatchesWork(task, item) || item.Input == nil {
		return workqueue.Change{}, checkpointConflict("Attach binding publication has no matching direct work")
	}
	reference, err := blueprintattachinputs.WorkInput(generation)
	if err != nil {
		return workqueue.Change{}, err
	}
	if reference != *item.Input {
		return workqueue.Change{}, checkpointConflict("Attach binding publication changed its captured input")
	}
	record, err := generation.PendingRecord(task.ID, task.CreatedAt)
	if err != nil {
		return workqueue.Change{}, err
	}
	return repository.publishBinding(ctx, workqueue.View{Task: workqueue.TaskSnapshot{Task: task},
		Item: workqueue.ItemSnapshot{Item: item}, ReadRevision: revision}, generation, record, nil)
}

func (repository *InputRepository) grantAdmission(ctx context.Context, generation blueprintattachinputs.Generation,
	grant blueprintattachinputs.BindingRecord, revision int64,
) ([]keyvalue.Condition, error) {
	key := attachments.AttachKey(grant.Record.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > revision {
		return nil, checkpointConflict("Attach grant target is not available")
	}
	current, err := attachments.DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	expected := attachments.CloneAttachRecord(grant.Record)
	expected.Status = core.AttachReady
	if current.Status != core.AttachReady || !attachments.SameBlueprintAttachCandidateRecord(expected, current) ||
		grant.Revision > 0 && read.Values[0].ModRevision != grant.Revision ||
		grant.Revision == 0 && current.TaskID != generation.ParentTaskID {
		return nil, checkpointConflict("Attach grant target changed after capture or has not completed")
	}
	return []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision},
		{Key: deletions.TombstoneKey("attach", current.ID)}}, nil
}
