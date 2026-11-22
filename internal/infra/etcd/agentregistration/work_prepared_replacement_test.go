package agentregistration

import (
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPreparedReplacementSeparatesCredentialAndExecutorGenerations(t *testing.T) {
	repository, store, ready := preparedReplacementFixture(t)
	execution, err := repository.MaintenanceExecution(t.Context(), ready.Record.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if execution.Binding.Generation != 2 || execution.Identity.Generation != 6 ||
		execution.ProcessGeneration != ([16]byte{6}) {
		t.Fatalf("maintenance execution = %#v", execution)
	}
	priorTaskID := ids.NewAt(ids.KindTask, ready.Record.CreatedAt, 89)
	if err := repository.BindPreparedReplacementExecution(t.Context(), priorTaskID, execution); err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, ready.Record.CreatedAt, 90)
	if err := repository.BindPreparedReplacementExecution(t.Context(), taskID, execution); err != nil {
		t.Fatalf("stale prior Task proof blocked independent replacement: %v", err)
	}
	candidate := replacePreparedCredential(t, repository, ready, 3)
	qualified, err := repository.PreparedReplacementExecution(
		t.Context(), taskID, ready.Record.ID, 2, candidate.Record.Generation,
	)
	if err != nil {
		t.Fatal(err)
	}
	if qualified.Identity != execution.Identity || qualified.ProcessGeneration != execution.ProcessGeneration {
		t.Fatalf("qualified execution = %#v, want %#v", qualified, execution)
	}
	if err := repository.ReleasePreparedReplacementExecution(
		t.Context(), taskID, ready.Record.ID, 2, candidate.Record.Generation,
	); err != nil {
		t.Fatal(err)
	}
	if value, err := store.Get(t.Context(), PreparedReplacementExecutionKey(ready.Record.ID, taskID)); err != nil ||
		value.Entry != nil {
		t.Fatalf("released proof = %#v, %v", value, err)
	}
	if value, err := store.Get(t.Context(), PreparedReplacementExecutionKey(ready.Record.ID, priorTaskID)); err != nil ||
		value.Entry == nil {
		t.Fatalf("independent prior Task proof was rewritten or removed: %#v, %v", value, err)
	}
}

func TestPreparedReplacementProofFailsClosedOnMissingOrReplacedProcess(t *testing.T) {
	repository, _, ready := preparedReplacementFixture(t)
	execution, err := repository.MaintenanceExecution(t.Context(), ready.Record.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	taskID := ids.NewAt(ids.KindTask, ready.Record.CreatedAt, 91)
	candidate := replacePreparedCredential(t, repository, ready, 3)
	if _, err := repository.PreparedReplacementExecution(
		t.Context(), taskID, ready.Record.ID, 2, candidate.Record.Generation,
	); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("missing prepared proof = %v", err)
	}

	repository, store, ready := preparedReplacementFixture(t)
	execution, err = repository.MaintenanceExecution(t.Context(), ready.Record.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	taskID = ids.NewAt(ids.KindTask, ready.Record.CreatedAt, 92)
	// A new executor process committed after the fixed read invalidates the
	// old nonce/revision rather than silently widening its work boundary.
	executor := workqueue.ExecutorRecord{Identity: execution.Identity,
		ProcessGeneration: [16]byte{7}, Capacity: 1, MaxConcurrentTasks: 1,
		MaxParallelOperationsPerTask: 1, Operations: []workexecution.Operation{workexecution.EntryConfiguration}}
	executor.Identity.Generation = 7
	raw, err := workqueue.EncodeExecutor(executor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: workqueue.ExecutorKey(executor.Identity), Value: raw,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := repository.BindPreparedReplacementExecution(t.Context(), taskID, execution); !errors.Is(
		err,
		errs.New(errs.KindStateConflict, ""),
	) {
		t.Fatalf("replaced executor accepted stale proof: %v", err)
	}
}

func preparedReplacementFixture(
	t *testing.T,
) (*Repository, *memoryTaskStore, keyvalue.Versioned[localagents.LocalAgentRecord]) {
	t.Helper()
	store := newMemoryTaskStore()
	repository, err := newRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 11, 22, 12, 0, 0, 0, time.UTC)
	record := localAgentTestRecord(localAgentTestToken(51), at)
	record.Generation = 2
	created, err := repository.CreateSingleton(t.Context(), record)
	if err != nil {
		t.Fatal(err)
	}
	ready, err := repository.MarkReady(t.Context(), record.ID, record.Generation, created.Revision,
		at.Add(time.Second), buildversion.BuildIdentity{Version: "test"})
	if err != nil {
		t.Fatal(err)
	}
	binding, err := repository.MaintenanceBinding(t.Context(), record.ID, record.Generation)
	if err != nil {
		t.Fatal(err)
	}
	executor := workqueue.ExecutorRecord{Identity: workexecution.Identity{Executor: workexecution.Agent,
		HostID: binding.HostID, AgentID: record.ID, Generation: 6}, ProcessGeneration: [16]byte{6},
		Ready: true, Capacity: 1, MaxConcurrentTasks: 1, MaxParallelOperationsPerTask: 1,
		Operations: []workexecution.Operation{workexecution.EntryConfiguration}}
	raw, err := workqueue.EncodeExecutor(executor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transact(t.Context(), nil, []keyvalue.Mutation{{
		Type: keyvalue.MutationPut, Key: workqueue.ExecutorKey(executor.Identity), Value: raw,
	}}); err != nil {
		t.Fatal(err)
	}
	return repository, store, ready
}

func replacePreparedCredential(
	t *testing.T,
	repository *Repository,
	ready keyvalue.Versioned[localagents.LocalAgentRecord],
	seed byte,
) keyvalue.Versioned[localagents.LocalAgentRecord] {
	t.Helper()
	token := localAgentTestToken(seed)
	digest := sha256.Sum256(token[:])
	replaced, err := repository.ReplaceGeneration(t.Context(), ready,
		"ghcr.io/groundplane/agent@sha256:"+strings.Repeat("b", 64),
		[]byte("replacement-encrypted-token"), base64.RawURLEncoding.EncodeToString(digest[:]), nil,
		ready.Record.TokenUpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return replaced
}
