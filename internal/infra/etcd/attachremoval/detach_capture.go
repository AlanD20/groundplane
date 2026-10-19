package attachremoval

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
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
		result.Clear()
		return DetachCapture{}, errs.New(
			errs.KindStateConflict,
			"Detach publication has lost its captured hook input authority",
		)
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
	return err == nil && view.Item.Item.Input != nil && input.TaskID == view.Task.Task.ID &&
		input.WorkItemID == view.Item.Item.ID && view.Item.Item.TaskID == view.Task.Task.ID && capture.TaskID == input.TaskID &&
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

// DetachCaptureAdmission joins immutable source ownership and active Secret
// retention into the caller's assignment or Start transaction. Live resource
// ownership is a separate guard from the native Detach publication owner.
func (repository *InputRepository) DetachCaptureAdmission(
	ctx context.Context,
	view workqueue.View,
) (workqueue.Change, error) {
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
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision},
	)
	return change, nil
}
