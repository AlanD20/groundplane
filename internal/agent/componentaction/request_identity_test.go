package componentaction

import (
	testing "testing"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: retry preserves the logical operation and sealed action but is a
// new Task attempt. Its managed-config transaction must therefore differ from
// a compensated predecessor while remaining stable across replay of one Task.
func TestManagedConfigRequestIdentityIsTaskAttemptScoped(t *testing.T) {
	t.Parallel()
	action := &agentpb.ComponentApply{
		ComponentId: "cmp_exact", ArtifactId: "cfg_exact", Generation: 7,
	}
	firstAssignment := testtaskassignment.Assignment{OperationID: "op_exact", TaskID: "task_first"}
	first := managedConfigRequest(
		firstAssignment,
		action,
		"config/Corefile",
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
	)
	replay := managedConfigRequest(
		firstAssignment,
		action,
		"config/Corefile",
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK,
	)
	retry := managedConfigRequest(testtaskassignment.Assignment{OperationID: "op_exact", TaskID: "task_retry"}, action,
		"config/Corefile",
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH,
	)
	if first.GetTransactionId() != replay.GetTransactionId() {
		t.Fatalf(
			"one Task attempt changed transaction identity: %q != %q",
			first.GetTransactionId(),
			replay.GetTransactionId(),
		)
	}
	if first.GetTransactionId() == retry.GetTransactionId() {
		t.Fatalf("retry reused compensated transaction identity %q", first.GetTransactionId())
	}
}
