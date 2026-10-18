package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func runnerLifecycleAttempt(remove bool) Attempt {
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	seed := int64(1)
	if remove {
		seed = 5
	}
	return Attempt{TaskID: ids.NewAt(ids.KindTask, at, seed),
		WorkItemID: ids.NewAt(ids.KindWorkItem, at, seed+1), AttemptID: ids.NewAt(ids.KindAttempt, at, seed+2),
		AttemptNumber: 1, Executor: workexecution.Identity{Executor: workexecution.Controller, Generation: 1},
		OwnershipNonce: "captured-owner", Plan: testPlan()}
}

type runnerTestAuthority struct {
	journal *memoryJournal
	err     error
}

func (authority *runnerTestAuthority) Grant(
	ctx context.Context,
	step runnerallocation.RunnerRuntimeStep,
	_ time.Time,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if authority.journal.progress.ActiveStep == nil || *authority.journal.progress.ActiveStep != step {
		return errs.New(errs.KindInternal, "Runner effect was not journaled before its grant")
	}
	return authority.err
}

// Lifecycle authority must not depend on available presentation storage.
// This fixture deliberately loses diagnostics; durable ownership is supplied by
// the separate journal/authority seams, not a permissive diagnostic writer.
func newRunnerTestDiagnostics(t *testing.T) *executiondiagnostics.Repository {
	t.Helper()
	store := runnerDiagnosticUnavailable{}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := executiondiagnostics.New(store, queue)
	if err != nil {
		t.Fatal(err)
	}
	return repository
}

type runnerDiagnosticUnavailable struct{}

func (runnerDiagnosticUnavailable) GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error) {
	return nil, errs.New(errs.KindStorageUnavailable, "diagnostic storage unavailable")
}
func (runnerDiagnosticUnavailable) Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	return nil, errs.New(errs.KindStorageUnavailable, "diagnostic storage unavailable")
}

func (runnerDiagnosticUnavailable) MeasureTransaction(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	return keyvalue.TransactionBudget{}, errs.New(errs.KindStorageUnavailable, "diagnostic storage unavailable")
}

func (runnerDiagnosticUnavailable) Transact(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return keyvalue.TransactionResult{}, errs.New(errs.KindStorageUnavailable, "diagnostic storage unavailable")
}

// Rationale: Abort/replacement may revoke a grant after the local journal issues
// a step. Issuance alone must never authorize the corresponding host command.
func TestRevokedRunnerGrantPreventsRuntimeMutation(t *testing.T) {
	journal := newMemoryJournal()
	runtime := &recordingRuntime{}
	lifecycle, err := NewLifecycle(journal, runtime, newRunnerTestDiagnostics(t))
	if err != nil {
		t.Fatal(err)
	}
	token, err := NewRegistrationToken([]byte("github-one-use-token"))
	if err != nil {
		t.Fatal(err)
	}
	revoked := errs.New(errs.KindStateConflict, "captured attempt was revoked")
	_, err = lifecycle.CreateWithEvidence(t.Context(), runnerLifecycleAttempt(false), token,
		&runnerTestAuthority{journal: journal, err: revoked})
	if !errors.Is(err, revoked) || len(runtime.steps) != 0 || len(runtime.resolved) != 0 || len(token.value) != 0 {
		t.Fatalf("revoked grant changed runtime: %v; effects=%v observations=%v", err, runtime.steps, runtime.resolved)
	}
}
