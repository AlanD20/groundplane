package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareFailedBinding changes only lifecycle visibility. It retains facts and
// reverse references: failure does not prove database or hook effects absent.
func (repository *InputRepository) PrepareFailedBinding(ctx context.Context, work workqueue.View) (workqueue.Change, error) {
	return repository.prepareBindingTransition(ctx, work, core.AttachPending, core.AttachFailed)
}

func (repository *InputRepository) prepareRetryBinding(ctx context.Context, work workqueue.View) (workqueue.Change, error) {
	return repository.prepareBindingTransition(ctx, work, core.AttachFailed, core.AttachPending)
}

func (repository *InputRepository) prepareBindingTransition(ctx context.Context, work workqueue.View, from, to core.AttachStatus) (workqueue.Change, error) {
	source, err := repository.LoadCaptured(ctx, work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer source.Clear()
	if source.Generation.Record.SettlesInController() && work.Task.Task.Action != workexecution.ActionAttach {
		return workqueue.Change{}, checkpointConflict("Attach failure has no host provisioning binding")
	}
	expected, err := source.Generation.Record.PendingRecord(work.Task.Task.ID, work.Task.Task.CreatedAt)
	if err != nil {
		return workqueue.Change{}, err
	}
	expected.Status = from
	key := attachments.AttachKey(expected.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > work.ReadRevision {
		return workqueue.Change{}, checkpointConflict("Attach failure lost its captured binding")
	}
	current, err := attachments.DecodeAttachRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !attachments.SameBlueprintAttachCandidateRecord(expected, current) {
		return workqueue.Change{}, checkpointConflict("Attach failure cannot overwrite a changed binding")
	}
	expected.Status = to
	raw, err := attachments.EncodeAttachRecord(expected)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: append(source.Conditions, keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}),
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, nil
}
