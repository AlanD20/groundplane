package app

import (
	"context"
	"math"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AtMaximumExecutionEpoch seeds the boundary before any hook checkpoints or
// closing report exist. It does not rewrite an already-closing assignment.
func (fixture *ExecutedArtifactFixture) AtMaximumExecutionEpoch(
	t *testing.T,
	claim etcd.TaskAssignment,
) etcd.TaskAssignment {
	t.Helper()
	ctx := context.Background()
	if claim.Assignment.Record.ExecutionMode != testtaskassignments.TaskExecutionModeForward {
		t.Fatal("maximum epoch fixture requires an active normal assignment")
	}
	lifecycle := testtaskjournal.TaskTimeoutIndexKey(claim.Task.Record.ID, claim.Assignment.Record.Deadline)
	record := claim.Assignment.Record
	record.ExecutionEpoch = math.MaxUint32
	encoded, err := testtaskassignments.EncodeTaskAssignment(record)
	if err != nil {
		t.Fatal(err)
	}
	taskBytes, err := etcd.EncodeTaskRecord(claim.Task.Record)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(record.TaskID), Value: taskBytes},
	}
	for _, key := range []string{testtaskjournal.TaskExecutionClaimKey(record.Executor, record.AgentID, record.TaskID), testtaskjournal.TaskAssignmentIndexKey(record.TaskID), lifecycle} {
		mutations = append(mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: encoded})
	}
	if result, err := fixture.store.Transact(ctx, nil, mutations); err != nil || !result.Succeeded {
		t.Fatalf("maximum epoch fixture: %v", err)
	}
	current, err := fixture.Tasks.GetTaskAssignment(ctx, record.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	// Rationale: without a stored final report, reconnect would require an
	// impossible next epoch and must still reject without changing any record.
	before := fixture.store.revision
	if _, err := fixture.Tasks.ReconnectAgentAssignment(ctx, current); !isKind(err, errs.KindStateConflict) ||
		fixture.store.revision != before {
		t.Fatalf("maximum active epoch did not reject without writes: %v", err)
	}
	return current
}
