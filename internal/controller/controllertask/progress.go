package controllertask

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type progressStore interface {
	AppendControllerTaskEvent(context.Context, taskjournal.TaskEventInput, time.Time) (etcd.TaskEventAppend, error)
}

// Progress records actual native calls in the existing Task journal. Journal
// failures retain the claim rather than terminalizing an incompletely recorded effect.
type Progress struct {
	store    progressStore
	ctx      context.Context
	identity taskjournal.TaskEventIdentity
	now      func() time.Time
	mu       sync.Mutex
	err      error
}

// NewProgress binds native operation observations to the owner's captured claim.
// Recording still goes through the ordinary Task journal; this grants no claim
// or execution authority and does not bypass event admission.
func NewProgress(ctx context.Context, store progressStore, claim etcd.TaskAssignment, now func() time.Time) *Progress {
	return &Progress{store: store, ctx: ctx, now: now, identity: taskjournal.TaskEventIdentity{
		AssignmentID: claim.Assignment.Record.AssignmentID, TaskID: claim.Task.Record.ID,
		Attempt: claim.Assignment.Record.ExecutionEpoch, Ordinal: claim.Task.Record.NextEventSequence,
	}}
}

func (progress *Progress) Run(ctx context.Context, stage string, action func(context.Context) error) error {
	if err := progress.record(stage, taskjournal.TaskEventStateRunning, nil); err != nil {
		return err
	}
	err := action(ctx)
	if recordErr := progress.Finish(stage, err); recordErr != nil {
		return recordErr
	}
	return err
}

// Finish records the observed action outcome. Callers with a typed result can
// report a failed candidate even when its successful restoration returned nil.
func (progress *Progress) Finish(stage string, err error) error {
	state := taskjournal.TaskEventStateCompleted
	if err != nil {
		state = taskjournal.TaskEventStateFailed
		if errors.Is(err, context.DeadlineExceeded) {
			state = taskjournal.TaskEventStateTimedOut
		} else if errors.Is(err, context.Canceled) {
			state = taskjournal.TaskEventStateAborted
		}
	}
	return progress.record(stage, state, taskprogress.CaptureControllerFailure(err))
}

// Start records an action whose outcome is supplied separately. A process-handoff
// wait has no outgoing completion; the successor reports its own readiness checks.
func (progress *Progress) Start(stage string) error {
	return progress.record(stage, taskjournal.TaskEventStateRunning, nil)
}

func (progress *Progress) record(stage string, state taskjournal.TaskEventState, failure *taskprogress.Failure) error {
	progress.mu.Lock()
	defer progress.mu.Unlock()
	if progress.err != nil {
		return progress.err
	}
	payload, err := json.Marshal(taskprogress.Payload{
		ControllerOperation: &taskprogress.ControllerOperation{Stage: stage}, Failure: failure,
	})
	if err != nil {
		progress.err = errs.Wrap(errs.KindInternal, err)
		return progress.err
	}
	ctx, cancel := context.WithTimeout(progress.ctx, 5*time.Second)
	defer cancel()
	_, progress.err = progress.store.AppendControllerTaskEvent(ctx, taskjournal.TaskEventInput{
		Identity: progress.identity, State: state, Payload: payload,
	}, progress.now().UTC())
	if progress.err == nil {
		progress.identity.Ordinal++
	}
	return progress.err
}

func (progress *Progress) Err() error {
	progress.mu.Lock()
	defer progress.mu.Unlock()
	return progress.err
}
