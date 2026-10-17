package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DetachCapture struct {
	Input      DetachInput
	Capture    WorkCapture
	Revision   int64
	Conditions []keyvalue.Condition
}

func (capture *DetachCapture) Clear() {
	if capture != nil {
		capture.Input.Clear()
		capture.Capture.Clear()
	}
}

func DetachOperation(view workqueue.View) string {
	return ids.DeriveAt(ids.KindOperation, view.Task.Task.CreatedAt, view.Item.Item.ID, "attach-removal-input")
}

func (repository *InputRepository) LoadDetachCapture(ctx context.Context, view workqueue.View) (DetachCapture, error) {
	input, conditions, err := repository.LoadDetach(ctx, view)
	if err != nil {
		return DetachCapture{}, err
	}
	result := DetachCapture{Input: input, Conditions: conditions}
	key := WorkCaptureKey(view.Item.Item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		result.Clear()
		return DetachCapture{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		result.Conditions = append(result.Conditions, keyvalue.Condition{Key: key})
		return result, nil
	}
	capture, err := DecodeWorkCapture(value.Value)
	if err != nil {
		result.Clear()
		return DetachCapture{}, err
	}
	result.Capture, result.Revision = capture, value.ModRevision
	if !matchesDetachCaptureSource(view, input, capture) || !matchesDetachHookCapture(input.Snapshot.Source, capture) {
		result.Clear()
		return DetachCapture{}, recordcodec.CorruptRecord()
	}
	result.Conditions = append(result.Conditions, keyvalue.Condition{Key: key, ModRevision: value.ModRevision})
	return result, nil
}

func matchesDetachCaptureSource(view workqueue.View, input DetachInput, capture WorkCapture) bool {
	digest, err := Digest(input.Snapshot.Source)
	return err == nil && view.Item.Item.Input != nil && capture.TaskID == input.TaskID &&
		capture.WorkItemID == input.WorkItemID && capture.OperationID == DetachOperation(view) &&
		capture.Action == *view.Item.Item.Input && capture.FrozenSHA256 == digest &&
		capture.BackingProjectID == input.Snapshot.Source.Attach.BackingProjectID
}

func matchesDetachHookCapture(source Input, capture WorkCapture) bool {
	requiresHook := false
	if NeedsAgent(source) && source.Backing.Desired.Adapter == "custom" && source.Backing.Desired.Hooks != nil {
		for _, input := range source.Backing.Desired.Hooks.Inputs {
			requiresHook = requiresHook || input.Generate == ""
		}
	}
	return requiresHook == (capture.Hook != nil)
}

func unstartedDetachCapture(view workqueue.View) bool {
	task, item := view.Task.Task, view.Item.Item
	return item.Operation == workexecution.Detach && item.Phase == workexecution.Ready && item.AttemptNumber == 0 &&
		item.ActiveAttemptID == "" && !item.ForwardRevoked && task.Stop == nil && task.FinishedAt == nil &&
		task.CollectionStartedAt == nil && task.PublicationComplete
}

// PrepareDetachPins retains only the exact Secret versions sealed for this
// work. Until PrepareDetachCapture publishes their active root, they cannot
// authorize dispatch or Start.
func (repository *InputRepository) PrepareDetachPins(ctx context.Context, view workqueue.View,
	capture WorkCapture,
) (tasksecretpins.Prepared, error) {
	if !unstartedDetachCapture(view) || capture.Hook == nil || len(capture.Sources) == 0 || capture.Pins != nil {
		return tasksecretpins.Prepared{}, errs.New(errs.KindStateConflict, "Detach Secret capture is not eligible for preparation")
	}
	current, err := repository.LoadDetachCapture(ctx, view)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	defer current.Clear()
	if current.Revision != 0 || !matchesDetachCaptureSource(view, current.Input, capture) ||
		!matchesDetachHookCapture(current.Input.Snapshot.Source, capture) {
		return tasksecretpins.Prepared{}, errs.New(errs.KindStateConflict, "Detach Secret source changed or was already captured")
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.BackingProjectID)
	if err != nil {
		return tasksecretpins.Prepared{}, err
	}
	return pins.PrepareOwned(ctx, capture.OperationID, capture.TaskID, capture.WorkItemID, capture.Sources, []tasksecretpins.Condition{
		{Key: workqueue.TaskKey(view.Task.Task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(view.Item.Item.ID), ModRevision: view.Item.Revision},
		{Key: WorkCaptureKey(view.Item.Item.ID)},
	})
}

func (repository *InputRepository) PrepareDetachCapture(ctx context.Context, view workqueue.View,
	capture WorkCapture, prepared tasksecretpins.Prepared,
) (workqueue.Change, error) {
	if !unstartedDetachCapture(view) || capture.Validate() != nil || (capture.Pins == nil) != prepared.IsZero() {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach input capture is not eligible for publication")
	}
	current, err := repository.LoadDetachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer current.Clear()
	if current.Revision != 0 || !matchesDetachCaptureSource(view, current.Input, capture) ||
		!matchesDetachHookCapture(current.Input.Snapshot.Source, capture) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach inputs changed or were already captured")
	}
	change := workqueue.Change{Conditions: current.Conditions}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(view.Task.Task.ID), ModRevision: view.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(view.Item.Item.ID), ModRevision: view.Item.Revision})
	if capture.Pins != nil {
		binding, err := prepared.Binding()
		if err != nil || binding != *capture.Pins {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
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
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: WorkCaptureKey(capture.WorkItemID), Value: raw})
	return change, nil
}

// DetachCaptureAdmission joins immutable source ownership and active Secret
// retention into the caller's assignment or Start transaction. Live resource
// ownership is a separate guard from the native Detach publication owner.
func (repository *InputRepository) DetachCaptureAdmission(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	capture, err := repository.LoadDetachCapture(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if capture.Revision <= 0 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach inputs have not been captured")
	}
	change := workqueue.Change{Conditions: capture.Conditions}
	if capture.Capture.Pins == nil {
		return change, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.Capture.BackingProjectID)
	if err != nil {
		return workqueue.Change{}, err
	}
	root, found, err := pins.LoadActive(ctx, capture.Capture.OperationID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Detach has lost its captured Secret retention")
	}
	condition, err := root.AdmissionCondition(*capture.Capture.Pins)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	return change, nil
}
