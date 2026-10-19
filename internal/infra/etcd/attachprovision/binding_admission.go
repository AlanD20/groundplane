package attachprovision

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PendingBindingConditions protects the captured binding and its references at
// every new effect grant. Hook output is stored separately until completion, so
// the binding's generated-input envelope must still match byte for byte.
func (repository *InputRepository) PendingBindingConditions(
	ctx context.Context,
	work workqueue.View,
) ([]keyvalue.Condition, error) {
	source, err := repository.LoadCaptured(ctx, work)
	if err != nil {
		return nil, err
	}
	defer source.Clear()
	generation := source.Generation.Record
	if generation.SettlesInController() && work.Task.Task.Action != workexecution.ActionAttach {
		return nil, checkpointConflict("Attach binding has no host provisioning authority")
	}
	record, err := generation.PendingRecord(work.Task.Task.ID, work.Task.Task.CreatedAt)
	if err != nil {
		return nil, err
	}
	keys := []string{attachments.AttachKey(record.ID), attachments.AttachFactsKey(record.ID),
		attachments.AttachNameKey(
			record.EnvironmentID,
			record.Name,
		), attachments.AttachOwnerKey(record.EnvironmentID, record.ID),
		attachments.AttachServiceKey(
			record.ServiceID,
			record.ID,
		), attachments.AttachBackingServiceKey(record.BackingServiceID, record.ID),
		attachments.AttachBackingProjectKey(record.BackingProjectID, record.ID)}
	for _, id := range record.GrantAttachIDs {
		keys = append(keys, attachments.AttachGrantedByKey(id, record.ID))
	}
	if len(record.GrantAttachIDs) > 0 {
		keys = append(keys, attachments.AttachDependentGrantKey(record.ID))
	}
	if !record.OwnsCredential() {
		keys = append(keys, attachments.AttachCredentialByKey(record.CredentialAttachID, record.ID))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != len(keys) {
		return nil, checkpointConflict("Attach binding ownership read is incomplete")
	}
	conditions := append([]keyvalue.Condition(nil), source.Conditions...)
	ownFacts := record.OwnsCredential() && (len(generation.FactSets) != 0 || record.HookBundle)
	for index, key := range keys {
		value := read.Values[index]
		if index == 1 && !ownFacts {
			if value != nil {
				return nil, checkpointConflict("Attach without owned facts acquired an unexpected fact envelope")
			}
			conditions = append(conditions, keyvalue.Condition{Key: key})
			continue
		}
		if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > work.ReadRevision {
			return nil, checkpointConflict("Attach binding or a required reference is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
		if index >= 2 && key != attachments.AttachDependentGrantKey(record.ID) && string(value.Value) != record.ID {
			return nil, checkpointConflict("Attach binding reference belongs to another resource")
		}
	}
	current, err := attachments.DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if !attachments.SameBlueprintAttachCandidateRecord(current, record) {
		return nil, checkpointConflict("Attach binding changed after its provisioning input was captured")
	}
	if ownFacts {
		facts, err := attachments.DecodeAttachEncryptedFacts(read.Values[1].Value)
		if err != nil {
			return nil, err
		}
		defer clear(facts.Ciphertext)
		if !sameEncryptedFacts(facts, generation.GeneratedInputs) {
			return nil, checkpointConflict("Attach generated facts changed after capture")
		}
	}
	if len(record.GrantAttachIDs) > 0 {
		encoded, err := attachments.EncodeAttachDependentGrantIndex(record.ID, record.GrantAttachIDs)
		if err != nil {
			return nil, err
		}
		// The dependent-grant index follows the seven fixed keys and one
		// reverse index per grant; credential references may follow it.
		matches := bytes.Equal(encoded, read.Values[7+len(record.GrantAttachIDs)].Value)
		clear(encoded)
		if !matches {
			return nil, checkpointConflict("Attach grant references changed after capture")
		}
	}
	credentialGuards, err := repository.CredentialConditions(ctx, generation, work.ReadRevision)
	if err != nil {
		return nil, err
	}
	conditions = append(conditions, credentialGuards...)
	if generation.Provision != nil {
		for _, grant := range generation.Provision.Grants {
			guards, err := repository.grantAdmission(ctx, generation, grant, work.ReadRevision)
			if err != nil {
				return nil, err
			}
			conditions = append(conditions, guards...)
		}
	}
	for _, id := range []string{record.EnvironmentID, record.BackingEnvironmentID} {
		fence, err := environmentfence.LoadOrdinary(ctx, repository.store, id, work.ReadRevision)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, fence.TransactionConditions()...)
	}
	conditions = append(conditions, keyvalue.Condition{Key: deletions.TombstoneKey("attach", record.ID)},
		keyvalue.Condition{Key: deletions.TombstoneKey(string(deletions.DeletionTargetZone), record.BackingNetworkID)})
	return conditions, nil
}
