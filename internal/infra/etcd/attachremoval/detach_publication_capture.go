package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// NewDetachCapture binds hook capture to the unpublished Task and immutable
// resource snapshot. The Controller fills the encrypted hook envelope from this
// snapshot before publication; no scheduler reads old Secret revisions later.
func NewDetachCapture(task workexecution.Task, item workexecution.Item, input DetachInput) (WorkCapture, error) {
	if task.Validate() != nil || item.Validate() != nil || item.Operation != workexecution.Detach || item.Input == nil ||
		input.TaskID != task.ID || input.WorkItemID != item.ID || item.TaskID != task.ID || item.TargetID != input.Snapshot.Source.Attach.ID {
		return WorkCapture{}, errs.New(errs.KindValidationFailed, "Detach capture requires matching Task, resource and input")
	}
	digest, err := Digest(input.Snapshot.Source)
	if err != nil {
		return WorkCapture{}, err
	}
	view := workqueue.View{Task: workqueue.TaskSnapshot{Task: task}, Item: workqueue.ItemSnapshot{Item: item}}
	return WorkCapture{Schema: 1, TaskID: task.ID, WorkItemID: item.ID, OperationID: DetachOperation(view),
		Action: *item.Input, FrozenSHA256: digest, BackingProjectID: input.Snapshot.Source.Attach.BackingProjectID}, nil
}

// PrepareDetachPublicationCapture must join Task/input/binding publication. Pin
// preparation verifies exact current Secret metadata and ciphertext under the
// same source comparisons; stale prepublication input fails rather than silently
// switching versions. No queued Task is exposed before capture is durable.
func (repository *InputRepository) PrepareDetachPublicationCapture(ctx context.Context, task workexecution.Task, item workexecution.Item,
	input DetachInput, capture WorkCapture, guards []keyvalue.Condition,
) (workqueue.Change, error) {
	view := workqueue.View{Task: workqueue.TaskSnapshot{Task: task}, Item: workqueue.ItemSnapshot{Item: item}}
	if ctx == nil || len(guards) == 0 || item.Input == nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Phase != workexecution.Ready || task.FinishedAt != nil || task.CollectionStartedAt != nil || task.Stop != nil ||
		capture.Pins != nil || !matchesDetachCaptureSource(view, input, capture) || !matchesDetachHookCapture(input.Snapshot.Source, capture) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach capture is not eligible for initial publication")
	}
	change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), guards...)}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID)}, keyvalue.Condition{Key: workqueue.ItemKey(item.ID)},
		keyvalue.Condition{Key: WorkCaptureKey(item.ID)}, keyvalue.Condition{Key: DetachInputKey(item.ID)})
	if len(capture.Sources) != 0 {
		pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.BackingProjectID)
		if err != nil {
			return workqueue.Change{}, err
		}
		owned := make([]tasksecretpins.Condition, len(change.Conditions))
		for index, guard := range change.Conditions {
			owned[index] = tasksecretpins.Condition{Key: guard.Key, ModRevision: guard.ModRevision, Prefix: guard.Prefix}
		}
		prepared, err := pins.PrepareOwned(ctx, capture.OperationID, task.ID, item.ID, capture.Sources, owned)
		if err != nil {
			return workqueue.Change{}, err
		}
		binding, err := prepared.Binding()
		if err != nil {
			return workqueue.Change{}, err
		}
		capture.Pins = &binding
		fragment, err := tasksecretpins.Activation(prepared)
		if err != nil {
			return workqueue.Change{}, err
		}
		conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
		if err != nil {
			fragment.Clear()
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, conditions...)
		change.Mutations = mutations
	}
	raw, err := EncodeWorkCapture(capture)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: WorkCaptureKey(item.ID), Value: raw})
	return change, nil
}
