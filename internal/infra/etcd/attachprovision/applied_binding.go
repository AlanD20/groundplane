package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (repository *InputRepository) PrepareControllerAppliedBinding(
	ctx context.Context,
	work workqueue.View,
) (workqueue.Change, error) {
	source, err := repository.LoadCaptured(ctx, work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	generation := source.Generation.Record
	consumer, err := generation.ControllerBinding(work.Task.Task.CreatedAt)
	if err != nil {
		return workqueue.Change{}, err
	}
	var facts *attachments.EncryptedFacts
	if consumer.OwnsCredential() && generation.GeneratedInputs.AttachID != "" {
		facts = &generation.GeneratedInputs
	}
	change, err := repository.prepareAppliedBinding(ctx, work, generation, consumer, facts)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, source.Conditions...)
	return change, nil
}

func (repository *InputRepository) prepareAppliedBinding(ctx context.Context, work workqueue.View,
	generation blueprintattachinputs.Generation, consumer attachments.Record, ownFacts *attachments.EncryptedFacts,
) (workqueue.Change, error) {
	if work.Item.Item.Input == nil {
		return workqueue.Change{}, checkpointConflict("Attach applied binding has no captured input")
	}
	binding := attachments.AppliedBinding{TaskID: work.Task.Task.ID, WorkItemID: work.Item.Item.ID,
		OperationID: generation.OperationID, Input: *work.Item.Item.Input, AuthoredSpecSHA256: generation.AuthoredSpecSHA256,
		Consumer: consumer, Credential: consumer, Facts: ownFacts}
	change := workqueue.Change{}
	if !consumer.OwnsCredential() {
		credential, facts, guards, err := repository.readReadyBinding(
			ctx,
			consumer.CredentialAttachID,
			work.ReadRevision,
		)
		if err != nil {
			return workqueue.Change{}, err
		}
		binding.Credential, binding.Facts = credential, facts
		defer binding.Clear()
		change.Conditions = append(change.Conditions, guards...)
	}
	for _, id := range binding.Credential.GrantAttachIDs {
		grant, facts, guards, err := repository.readReadyBinding(ctx, id, work.ReadRevision)
		if facts != nil {
			clear(facts.Ciphertext)
		}
		if err != nil {
			return workqueue.Change{}, err
		}
		binding.Grants = append(binding.Grants, grant)
		change.Conditions = append(change.Conditions, guards...)
	}
	key := attachments.AppliedBindingKey(consumer.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 {
		return workqueue.Change{}, checkpointConflict("Attach applied binding read is incomplete")
	}
	if prior := read.Values[0]; prior != nil {
		if prior.Key != key || prior.ModRevision <= 0 || prior.ModRevision > work.ReadRevision {
			return workqueue.Change{}, checkpointConflict("Attach applied binding revision is invalid")
		}
		previous, err := attachments.DecodeAppliedBinding(prior.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		previous.Clear()
		if previous.Consumer.ID != consumer.ID || previous.Consumer.EnvironmentID != consumer.EnvironmentID {
			return workqueue.Change{}, checkpointConflict("Attach applied binding belongs to another resource")
		}
	}
	raw, err := attachments.EncodeAppliedBinding(binding)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])},
	)
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}
	return change, nil
}

func (repository *InputRepository) readReadyBinding(
	ctx context.Context,
	id string,
	revision int64,
) (attachments.Record, *attachments.EncryptedFacts, []keyvalue.Condition, error) {
	keys := []string{attachments.AttachKey(id), attachments.AttachFactsKey(id)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return attachments.Record{}, nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] == nil {
		return attachments.Record{}, nil, nil, checkpointConflict("Attach applied credential source is unavailable")
	}
	guards := make([]keyvalue.Condition, 0, 2)
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
			return attachments.Record{}, nil, nil, checkpointConflict(
				"Attach applied credential source changed revision",
			)
		}
		guards = append(guards, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	record, err := attachments.DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return attachments.Record{}, nil, nil, err
	}
	if _, err := attachments.BindingDigest(record); err != nil || record.ID != id {
		return attachments.Record{}, nil, nil, checkpointConflict("Attach applied credential source is not ready")
	}
	var facts *attachments.EncryptedFacts
	if read.Values[1] != nil {
		decoded, err := attachments.DecodeAttachEncryptedFacts(read.Values[1].Value)
		if err != nil {
			return attachments.Record{}, nil, nil, err
		}
		if decoded.AttachID != id {
			clear(decoded.Ciphertext)
			return attachments.Record{}, nil, nil, checkpointConflict(
				"Attach applied facts belong to another credential",
			)
		}
		facts = &decoded
	}
	return record, facts, guards, nil
}
