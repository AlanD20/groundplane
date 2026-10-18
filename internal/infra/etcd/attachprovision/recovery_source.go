package attachprovision

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func RecoveryKey(workID string) string { return "/v1/attach-provision-recovery/" + workID }

type RecoverySnapshot struct {
	Source   attachexecution.ProvisionRecovery
	Revision int64
}

// prepareRecoverySource joins the original failure and inspection authority in
// the same commit that releases forward capacity. Resource claims remain held.
func (repository *InputRepository) prepareRecoverySource(ctx context.Context, view workqueue.ResumeView,
	failure attachexecution.ProvisionFailure, result workexecution.Completion, at time.Time,
) (workqueue.Change, error) {
	if result.EffectsSettled || failure.Checkpoint == nil {
		return workqueue.Change{}, checkpointConflict("Attach recovery requires retained command effects")
	}
	_, forward, err := workexecution.Finish(view.Work.Item.Item, view.Attempt.Attempt, result, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	raw, err := attachexecution.EncodeProvisionRecovery(attachexecution.ProvisionRecovery{
		Forward: forward, Failure: failure, Evidence: result.Evidence,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	key := RecoveryKey(forward.WorkItemID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 {
		clear(raw)
		return workqueue.Change{}, checkpointConflict("Attach recovery source read is incomplete")
	}
	if value := read.Values[0]; value != nil {
		previous, err := attachexecution.DecodeProvisionRecovery(value.Value)
		if err != nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision ||
			previous.Forward.TaskID != forward.TaskID || previous.Forward.WorkItemID != forward.WorkItemID ||
			previous.Forward.Input != forward.Input || previous.Forward.Executor.HostID != forward.Executor.HostID ||
			previous.Forward.Number >= forward.Number {
			clear(raw)
			return workqueue.Change{}, checkpointConflict("Attach recovery cannot replace unrelated or newer failure authority")
		}
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: raw}}}, nil
}

func (repository *InputRepository) LoadRecovery(ctx context.Context, view workqueue.View) (RecoverySnapshot, error) {
	item := view.Item.Item
	if item.Operation != workexecution.Attach || item.Input == nil || view.ReadRevision <= 0 {
		return RecoverySnapshot{}, checkpointConflict("Attach recovery requires captured provisioning work")
	}
	key := RecoveryKey(item.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if read == nil || read.ReadRevision != view.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil ||
		read.Values[0].Key != key || read.Values[0].ModRevision <= 0 || read.Values[0].ModRevision > view.ReadRevision {
		return RecoverySnapshot{}, checkpointConflict("Attach recovery source is unavailable")
	}
	source, err := attachexecution.DecodeProvisionRecovery(read.Values[0].Value)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	if source.Forward.TaskID != view.Task.Task.ID || source.Forward.WorkItemID != item.ID ||
		source.Forward.Input != *item.Input || source.Forward.Executor.HostID != item.Placement.HostID ||
		source.Failure.Authority.AttachID != item.TargetID || source.Forward.Number > item.AttemptNumber {
		return RecoverySnapshot{}, checkpointConflict("Attach recovery differs from its captured work")
	}
	if source.Forward.Number == item.AttemptNumber && (item.ActiveAttemptID != "" || item.Result == nil || *item.Result != source.Evidence ||
		(item.Cleanup != workexecution.CleanupRequired && item.Cleanup != workexecution.CleanupStopped) ||
		(item.Phase != workexecution.Ready && item.Phase != workexecution.Waiting && item.Phase != workexecution.Attention)) {
		return RecoverySnapshot{}, checkpointConflict("Attach failure is not awaiting effect inspection")
	}
	return RecoverySnapshot{Source: source, Revision: read.Values[0].ModRevision}, nil
}
