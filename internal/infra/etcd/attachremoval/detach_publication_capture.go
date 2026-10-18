package attachremoval

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
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

// PrepareDetachPublicationCapture must join input/binding publication. Pin
// preparation verifies exact current Secret metadata and ciphertext under the
// same source comparisons; stale prepublication input fails rather than silently
// switching versions. A direct action uses an absent Task; a graph batch uses
// the exact accepted Task revision while its execution barrier remains closed.
func (repository *InputRepository) PrepareDetachPublicationCapture(ctx context.Context, snapshot workqueue.TaskSnapshot, item workexecution.Item,
	input DetachInput, capture WorkCapture, guards []keyvalue.Condition,
) (workqueue.Change, error) {
	task := snapshot.Task
	view := workqueue.View{Task: snapshot, Item: workqueue.ItemSnapshot{Item: item}}
	if ctx == nil || len(guards) == 0 || item.Input == nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		snapshot.Revision < 0 || task.Validate() != nil || task.PublicationComplete || item.Validate() != nil ||
		item.TaskID != task.ID || item.Operation != workexecution.Detach ||
		item.Phase != workexecution.Ready || task.FinishedAt != nil || task.CollectionStartedAt != nil || task.Stop != nil ||
		capture.Pins != nil || !matchesDetachCaptureSource(view, input, capture) || !matchesDetachHookCapture(input.Snapshot.Source, capture) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach capture is not eligible for initial publication")
	}
	if snapshot.Revision > 0 && (input.Cascade == nil || task.Action != workexecution.ActionRemove ||
		input.Cascade.Validate(input.Snapshot) != nil || input.Cascade.ZoneID != task.TargetID ||
		task.ExpectedItems != input.Cascade.Count+1 || task.Scope.TenantID != "" ||
		task.Scope.ProjectID != input.Snapshot.Source.Attach.BackingProjectID ||
		task.Scope.EnvironmentID != input.Snapshot.Source.Attach.BackingEnvironmentID) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "accepted Detach capture requires its closed Backing Zone graph")
	}
	change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), guards...)}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: snapshot.Revision}, keyvalue.Condition{Key: workqueue.ItemKey(item.ID)},
		keyvalue.Condition{Key: WorkCaptureKey(item.ID)})
	if snapshot.Revision == 0 {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: DetachInputKey(item.ID)})
	} else {
		view.ReadRevision = snapshot.Revision
		prepared, err := repository.LoadPreparedCascadeCapture(ctx, view)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer prepared.Clear()
		left, leftErr := recordcodec.Encode("prepared-cascade-hook", capture)
		right, rightErr := recordcodec.Encode("prepared-cascade-hook", prepared.Capture)
		equal := leftErr == nil && rightErr == nil && bytes.Equal(left, right)
		clear(left)
		clear(right)
		if !equal {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Backing Zone publication changed its presealed hook capture")
		}
		change.Conditions = append(change.Conditions, prepared.Conditions...)
	}
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
	if snapshot.Revision > 0 {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: preparedCascadeCaptureKey(item.ID)})
	}
	return change, nil
}
