package controllertask

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: expiry cannot acknowledge away a partial native or Agent update;
// the recovery owner must prove a settled outcome before the claim is removed.
func TestExpiredUpdateRunsRecoveryBeforeAcknowledgement(t *testing.T) {
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	for _, resource := range []string{etcd.TaskResourceAgent, etcd.TaskResourceController} {
		t.Run(resource, func(t *testing.T) {
			claim := controllerClaim(now.Add(-time.Hour), now.Add(-time.Minute))
			claim.Task.Record.Type = etcd.TaskUpdate
			claim.Task.Record.Params = map[string]string{etcd.TaskResourceKindParam: resource}
			store := &fakeStore{claims: []etcd.TaskAssignment{claim}}
			handler := &fakeHandler{}
			runner := testRunner(t, store, handler, now)
			updates := &fakeUpdateExecutor{status: etcd.TaskStatusTimedOut}
			runner.updates = updates
			if err := runner.Restore(context.Background()); err != nil {
				t.Fatal(err)
			}
			if updates.restored != claim.Task.Record.ID || updates.executions != 0 || store.ackCalls != 0 {
				t.Fatal("startup restoration executed or acknowledged before the channel started")
			}
			if _, err := runner.runOne(context.Background()); err != nil {
				t.Fatal(err)
			}
			if updates.executions != 1 || store.ackStatus != etcd.TaskStatusTimedOut || handler.calls != 0 {
				t.Fatal("expired update bypassed recovery")
			}
		})
	}
}

// Rationale: a storage or host recovery failure must keep the same durable
// claim retryable, including after its original deadline has elapsed.
func TestUnsettledUpdateDoesNotAcknowledgeClaim(t *testing.T) {
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	claim := controllerClaim(now.Add(-time.Hour), now.Add(-time.Minute))
	claim.Task.Record.Type = etcd.TaskUpdate
	claim.Task.Record.Params = map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceAgent}
	store := &fakeStore{claims: []etcd.TaskAssignment{claim}}
	runner := testRunner(t, store, &fakeHandler{}, now)
	runner.updates = &fakeUpdateExecutor{err: errs.New(errs.KindStorageUnavailable, "unresolved")}
	if _, err := runner.runOne(context.Background()); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) ||
		store.ackCalls != 0 {
		t.Fatalf("unsettled recovery = %v, acknowledgements %d", err, store.ackCalls)
	}
}

// Rationale: post-activation Abort must consult the recovery owner; cancelling
// the execution context directly could kill the only in-process recovery path.
func TestCommittedUpdateRejectsAbortWithoutCancellingExecution(t *testing.T) {
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	claim := controllerClaim(now, now.Add(time.Minute))
	claim.Task.Record.Type = etcd.TaskUpdate
	claim.Task.Record.Params = map[string]string{etcd.TaskResourceKindParam: etcd.TaskResourceController}
	store := &fakeStore{claims: []etcd.TaskAssignment{claim}}
	runner := testRunner(t, store, &fakeHandler{}, now)
	updates := &fakeUpdateExecutor{
		started: make(chan struct{}), finish: make(chan struct{}), status: etcd.TaskStatusCompleted,
		abortErr: errs.New(errs.KindResourceInUse, "activation is committed"),
	}
	runner.updates = updates
	done := make(chan error, 1)
	go func() { _, err := runner.runOne(context.Background()); done <- err }()
	<-updates.started
	if err := runner.AbortTask(context.Background(), claim.Task.Record.ID); !errors.Is(
		err, errs.New(errs.KindResourceInUse, ""),
	) {
		t.Fatalf("committed abort = %v", err)
	}
	close(updates.finish)
	if err := <-done; err != nil || store.ackStatus != etcd.TaskStatusCompleted {
		t.Fatalf("committed execution = %v, status %s", err, store.ackStatus)
	}
}

type fakeUpdateExecutor struct {
	restored   string
	executions int
	status     etcd.TaskStatus
	err        error
	abortErr   error
	started    chan struct{}
	finish     chan struct{}
}

func (updates *fakeUpdateExecutor) Restore(_ context.Context, claim etcd.TaskAssignment) error {
	updates.restored = claim.Task.Record.ID
	return nil
}

func (updates *fakeUpdateExecutor) Execute(
	ctx context.Context, _ etcd.TaskRecord, _ time.Time,
) (etcd.TaskStatus, error) {
	updates.executions++
	if updates.started != nil {
		close(updates.started)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-updates.finish:
		}
	}
	return updates.status, updates.err
}

func (updates *fakeUpdateExecutor) Abort(context.Context, string) error {
	return updates.abortErr
}
