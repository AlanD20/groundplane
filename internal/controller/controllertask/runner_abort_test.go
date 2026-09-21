package controllertask

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type abortBlockingHandler struct {
	started chan struct{}
}

func (handler *abortBlockingHandler) Execute(ctx context.Context, _ etcd.TaskRecord) error {
	close(handler.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestRunnerOperatorAbortCancelsAndDurablyAcknowledgesTheActiveTask(t *testing.T) {
	// Rationale: native Controller work must distinguish an operator abort from
	// process shutdown so only the former commits the aborted terminal state.
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, time.UTC)
	claim := controllerClaim(now, now.Add(time.Minute))
	store := &fakeStore{claims: []etcd.TaskAssignment{claim}}
	handler := &abortBlockingHandler{started: make(chan struct{})}
	runner := testRunner(t, store, handler, now)
	runResult := make(chan error, 1)
	go func() {
		_, err := runner.runOne(context.Background())
		runResult <- err
	}()
	<-handler.started
	if err := runner.AbortTask(context.Background(), claim.Task.Record.ID); err != nil {
		t.Fatalf("AbortTask() error = %v", err)
	}
	if err := <-runResult; err != nil {
		t.Fatalf("runOne() error = %v", err)
	}
	if store.ackCalls != 1 || store.ackTaskID != claim.Task.Record.ID ||
		store.ackStatus != testtaskjournal.TaskStatusAborted {
		t.Fatalf("abort acknowledgement = %d/%s/%s", store.ackCalls, store.ackTaskID, store.ackStatus)
	}
}
