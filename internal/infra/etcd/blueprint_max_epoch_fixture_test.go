package etcd

import (
	"context"
	"math"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// AtMaximumExecutionEpoch seeds the boundary before any hook checkpoints or
// closing report exist. It does not rewrite an already-closing assignment.
func (fixture *ExecutedArtifactFixture) AtMaximumExecutionEpoch(t *testing.T, claim TaskAssignment) TaskAssignment {
	t.Helper()
	ctx := context.Background()
	old, err := encodeTaskAssignment(claim.Assignment.Record)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, _, _, err := fixture.Tasks.assignmentLifecycleIndexAtRevision(ctx, claim.Assignment.Record,
		&KeyValue{Value: old, ModRevision: claim.Assignment.Revision}, claim.Task.ReadRevision)
	if err != nil {
		t.Fatal(err)
	}
	record := claim.Assignment.Record
	record.ExecutionEpoch = math.MaxUint32
	encoded, err := encodeTaskAssignment(record)
	if err != nil {
		t.Fatal(err)
	}
	taskBytes, err := encodeTaskRecord(claim.Task.Record)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []Mutation{{Type: MutationPut, Key: taskKey(record.TaskID), Value: taskBytes}}
	for _, key := range []string{taskExecutionClaimKey(record.Executor, record.AgentID, record.TaskID),
		taskAssignmentIndexKey(record.TaskID), lifecycle} {
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: encoded})
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
