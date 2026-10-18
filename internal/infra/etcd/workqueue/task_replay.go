package workqueue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func replayBindingKey(taskID string) string { return root + "tasks/" + taskID + "/replay" }

// Only the locator is stored here. Encrypted request intent and the accepted
// response stay in the existing idempotency record, not in Task history.
func bindReplay(change *Change, task workexecution.Task, marker idempotency.IdempotencyMarker) error {
	if idempotency.ValidateIdempotencyMarker(marker) != nil || marker.Kind != idempotency.IdempotencyMarkerTask ||
		marker.State != idempotency.IdempotencyMarkerPending || marker.TaskID != task.ID ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(task.CreatedAt) {
		return invalid("execution publication requires its matching pending request replay")
	}
	value, err := encode("execution_replay", marker.Locator)
	if err != nil {
		return err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: replayBindingKey(task.ID)})
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: replayBindingKey(task.ID), Value: value})
	return nil
}

// PrepareTaskFinish joins the public outcome and request replay lifecycle. The
// caller joins any operation-owned aggregate settlement before committing.
// A reopened Task keeps its original accepted response; finishing it again only
// advances the marker's terminal state and retention window.
func (repository *Repository) PrepareTaskFinish(ctx context.Context, task TaskSnapshot,
	at, retainUntil time.Time,
) (Change, error) {
	next, err := workexecution.CompleteTask(task.Task, at, retainUntil)
	if err != nil {
		return Change{}, err
	}
	return repository.prepareFinishedReplay(ctx, task, next, at)
}

// PreparePlanningRejection joins a no-work failure with request replay and Task
// retention. The operation owner must join its saved failure evidence and
// release of planning-only resource fences. No execution graph can be discarded.
func (repository *Repository) PreparePlanningRejection(ctx context.Context, task TaskSnapshot,
	outcome workexecution.Outcome, evidence workexecution.Input, at, retainUntil time.Time,
) (Change, error) {
	next, err := workexecution.RejectPlanning(task.Task, outcome, evidence, at, retainUntil)
	if err != nil {
		return Change{}, err
	}
	change, err := repository.prepareFinishedReplay(ctx, task, next, at)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: TaskItemsPrefix(task.Task.ID), Prefix: true})
	return change, nil
}

func (repository *Repository) prepareFinishedReplay(ctx context.Context, task TaskSnapshot,
	next workexecution.Task, at time.Time,
) (Change, error) {
	read, err := repository.read(ctx, []string{replayBindingKey(task.Task.ID)}, 0)
	if err != nil {
		return Change{}, err
	}
	if read.Values[0] == nil {
		return Change{}, corrupt()
	}
	locator, err := recordcodec.Decode[idempotency.IdempotencyLocator](read.Values[0].Value, "execution_replay")
	if err != nil {
		return Change{}, err
	}
	markerKey, err := idempotency.IdempotencyMarkerKey(locator)
	if err != nil {
		return Change{}, corrupt()
	}
	markers, err := repository.read(ctx, []string{markerKey}, read.ReadRevision)
	if err != nil {
		return Change{}, err
	}
	if markers.Values[0] == nil {
		return Change{}, corrupt()
	}
	marker, err := idempotency.DecodeIdempotencyMarker(markers.Values[0].Value, locator)
	if err != nil {
		return Change{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if marker.Kind != idempotency.IdempotencyMarkerTask || marker.TaskID != task.Task.ID ||
		!marker.CreatedAt.Equal(task.Task.CreatedAt) || at.Before(marker.UpdatedAt) {
		return Change{}, corrupt()
	}
	previousRetention := marker.RetainUntil
	marker.State = idempotency.IdempotencyMarkerFailed
	if next.Outcome == workexecution.Succeeded {
		marker.State = idempotency.IdempotencyMarkerCompleted
	}
	marker.UpdatedAt, marker.TerminalAt, marker.RetainUntil = at, at, at.Add(idempotency.MarkerRetention)
	value, err := idempotency.EncodeIdempotencyMarker(marker)
	if err != nil {
		return Change{}, err
	}
	retentionKey, err := idempotency.IdempotencyRetentionKey(markerKey, marker.RetainUntil)
	if err != nil {
		return Change{}, err
	}
	reference, err := json.Marshal(idempotency.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
	if err != nil {
		return Change{}, err
	}
	change, err := taskChange(task, next)
	if err != nil {
		return Change{}, err
	}
	change.Conditions = append(change.Conditions,
		keyvalue.Condition{Key: replayBindingKey(task.Task.ID), ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: markerKey, ModRevision: markers.Values[0].ModRevision})
	change.Mutations = append(change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: markerKey, Value: value},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: retentionKey, Value: reference})
	if !previousRetention.IsZero() && !previousRetention.Equal(marker.RetainUntil) {
		oldKey, err := idempotency.IdempotencyRetentionKey(markerKey, previousRetention)
		if err != nil {
			return Change{}, err
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: oldKey})
	}
	return change, nil
}
