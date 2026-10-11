package app

import (
	"context"
	"math"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprintunits "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
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
	// The native child claim and assignment share an epoch. Seed both boundary
	// records together; a mismatched unit would test corruption, not exhaustion.
	unitKey := testblueprintunits.ExecutionKey(claim.Task.Record.Owner.EnvironmentID, claim.Task.Record.PlanID)
	unitRead, err := fixture.store.Get(ctx, unitKey)
	if err != nil || unitRead == nil || unitRead.Entry == nil {
		t.Fatalf("maximum epoch unit read: %v", err)
	}
	unit, err := testblueprintunits.DecodeExecution(unitRead.Entry.Value)
	if err != nil || unit.TaskID != record.TaskID || unit.Epoch != int64(claim.Assignment.Record.ExecutionEpoch) {
		t.Fatalf("maximum epoch unit identity: %v", err)
	}
	unit.Epoch = int64(record.ExecutionEpoch)
	unitBytes, err := testblueprintunits.EncodeExecution(unit)
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(mutations, testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: unitKey, Value: unitBytes})
	epochKey := testblueprintunits.EpochKey(unit.EnvironmentID)
	epochRead, err := fixture.store.Get(ctx, epochKey)
	if err != nil || epochRead == nil || epochRead.Entry == nil {
		t.Fatalf("maximum epoch ledger read: %v", err)
	}
	epoch, err := testblueprintunits.DecodeEpoch(epochRead.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	epoch.Sequence++
	epochBytes, err := testblueprintunits.EncodeEpoch(epoch)
	if err != nil {
		t.Fatal(err)
	}
	mutations = append(
		mutations,
		testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: epochKey, Value: epochBytes},
	)
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
