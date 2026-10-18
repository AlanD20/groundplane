package attachremoval

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

func preparedCascadeCaptureKey(workID string) string {
	return DetachInputPrefix(workID) + "prepared-hook"
}

// StageCascadeCapture stores encrypted admission inputs before Task acceptance.
// It activates no pins and grants no work: publication later verifies the exact
// Secret versions and joins active capture, binding ownership and queue item.
func (repository *InputRepository) StageCascadeCapture(ctx context.Context, task workexecution.Task, item workexecution.Item,
	input DetachInput, capture WorkCapture, admission []keyvalue.Condition,
) error {
	view := workqueue.View{Task: workqueue.TaskSnapshot{Task: task}, Item: workqueue.ItemSnapshot{Item: item}}
	if task.Validate() != nil || task.PublicationComplete || task.Action != workexecution.ActionRemove ||
		input.Cascade == nil || input.Cascade.ZoneID != task.TargetID || task.Scope.TenantID != "" ||
		item.Validate() != nil || item.Input == nil || item.TaskID != task.ID || item.Operation != workexecution.Detach ||
		len(admission) == 0 || capture.Pins != nil || capture.validate(false) != nil ||
		!matchesDetachCaptureSource(view, input, capture) || !matchesDetachHookCapture(input.Snapshot.Source, capture) {
		return checkpointConflict("Backing Zone capture requires its unpublished immutable consumer input")
	}
	raw, err := recordcodec.Encode("prepared-cascade-hook", capture)
	if err != nil {
		return err
	}
	defer clear(raw)
	if len(raw) > MaximumInputBytes {
		return checkpointConflict("Backing Zone encrypted hook capture exceeds its storage bound")
	}
	key := preparedCascadeCaptureKey(item.ID)
	read, err := repository.read(ctx, []string{key}, 0)
	if err != nil {
		return err
	}
	defer keyvalue.ClearValues(read.Values)
	if saved := read.Values[0]; saved != nil {
		if !bytes.Equal(saved.Value, raw) {
			return checkpointConflict("Backing Zone encrypted hook capture already differs")
		}
		view.ReadRevision = read.ReadRevision
		loaded, _, err := repository.LoadDetach(ctx, view)
		loaded.Clear()
		return err
	}
	guards := append([]keyvalue.Condition(nil), admission...)
	guards = append(guards, keyvalue.Condition{Key: workqueue.TaskKey(task.ID)},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID)}, keyvalue.Condition{Key: key})
	reference, change, err := repository.StageDetach(ctx, item.Input.ID, input, guards)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	if reference != *item.Input {
		return checkpointConflict("Backing Zone consumer capture differs from its graph input")
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: raw})
	result, err := repository.transactions.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return checkpointConflict("Backing Zone capture admission changed before storage")
	}
	return nil
}

func (repository *InputRepository) LoadPreparedCascadeCapture(ctx context.Context, view workqueue.View) (DetachCapture, error) {
	input, guards, err := repository.LoadDetach(ctx, view)
	if err != nil {
		return DetachCapture{}, err
	}
	result := DetachCapture{Input: input, Conditions: guards}
	key := preparedCascadeCaptureKey(view.Item.Item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		result.Clear()
		return DetachCapture{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || len(read.Values[0].Value) > MaximumInputBytes {
		result.Clear()
		return DetachCapture{}, checkpointConflict("Backing Zone encrypted hook capture is missing")
	}
	capture, err := recordcodec.Decode[WorkCapture](read.Values[0].Value, "prepared-cascade-hook")
	result.Capture = capture
	if err != nil || input.Cascade == nil || capture.Pins != nil || capture.validate(false) != nil ||
		!matchesDetachCaptureSource(view, input, capture) || !matchesDetachHookCapture(input.Snapshot.Source, capture) {
		result.Clear()
		return DetachCapture{}, recordcodec.CorruptRecord()
	}
	if capture.Hook != nil {
		result.Capture.Hook.SecretSources = append([]tasksecretpinrecord.Record(nil), capture.Sources...)
	}
	result.Revision = read.Values[0].ModRevision
	result.Conditions = append(result.Conditions, keyvalue.Condition{Key: key, ModRevision: result.Revision})
	return result, nil
}
