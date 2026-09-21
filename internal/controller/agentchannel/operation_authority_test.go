package agentchannel

import (
	taskcontract "github.com/AlanD20/groundplane/internal/controller/taskcontract"
	etcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	testing "testing"
)

// Rationale: the Agent channel must preserve every closed Task/plan operation
// pairing while rejecting a plan sealed for another Task type.
func TestOperationMatchesTaskAcceptsClosedPairingsAndRejectsCrossPairs(t *testing.T) {
	pairs := []struct {
		taskType  testtaskjournal.TaskType
		operation agentpb.PlanOperation
	}{
		{taskType: testtaskjournal.TaskDeploy, operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY},
		{taskType: testtaskjournal.TaskRollback, operation: agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK},
		{taskType: testtaskjournal.TaskStart, operation: agentpb.PlanOperation_PLAN_OPERATION_START},
		{taskType: testtaskjournal.TaskStop, operation: agentpb.PlanOperation_PLAN_OPERATION_STOP},
		{taskType: testtaskjournal.TaskDestroy, operation: agentpb.PlanOperation_PLAN_OPERATION_DESTROY},
		{taskType: testtaskjournal.TaskRemove, operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE},
		{taskType: testtaskjournal.TaskCreate, operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE},
		{taskType: testtaskjournal.TaskAttach, operation: agentpb.PlanOperation_PLAN_OPERATION_ATTACH},
		{taskType: testtaskjournal.TaskDetach, operation: agentpb.PlanOperation_PLAN_OPERATION_DETACH},
		{taskType: testtaskjournal.TaskBackup, operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP},
		{taskType: testtaskjournal.TaskBackupPrune, operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE},
	}
	for _, pair := range pairs {
		if !operationMatchesTask(pair.operation, etcd.TaskRecord{Type: pair.taskType}) {
			t.Errorf("operationMatchesTask(%s, %q) = false, want true", pair.operation, pair.taskType)
		}
	}
	for _, pair := range pairs {
		for _, other := range pairs {
			if pair.taskType == other.taskType {
				continue
			}
			if operationMatchesTask(pair.operation, etcd.TaskRecord{Type: other.taskType}) {
				t.Errorf(
					"operationMatchesTask(%s, %q) = true for cross-pair with %q",
					pair.operation,
					other.taskType,
					pair.taskType,
				)
			}
		}
	}
	backingCreation := etcd.TaskRecord{
		Type:   testtaskjournal.TaskUpdate,
		Params: map[string]string{testtaskjournal.TaskBackingServiceCreationParam: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}
	if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE, backingCreation) {
		t.Error("backing-service TaskUpdate did not accept its Environment-create plan")
	}
	componentUpdate := etcd.TaskRecord{
		Type:   testtaskjournal.TaskUpdate,
		Params: map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceComponent},
	}
	if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, componentUpdate) {
		t.Error("Component TaskUpdate did not accept its Component-apply plan")
	}
	if operationMatchesTask(
		agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		etcd.TaskRecord{Type: testtaskjournal.TaskUpdate},
	) {
		t.Error("ordinary TaskUpdate accepted a Component-apply plan")
	}
	volumeCreation := etcd.TaskRecord{
		Type:   testtaskjournal.TaskCreate,
		Params: map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceVolume},
	}
	if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, volumeCreation) {
		t.Error("Volume TaskCreate did not accept its reconciliation plan")
	}
	if operationMatchesTask(
		agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		etcd.TaskRecord{Type: testtaskjournal.TaskCreate},
	) {
		t.Error("ordinary TaskCreate accepted a reconciliation plan")
	}
}

// Rationale: an Environment TaskUpdate's durable Compose procedure is the
// assignment authority. An absent or different marker must not let a plan
// cross between Blueprint apply and ordinary reconciliation semantics.
func TestOperationMatchesTaskRequiresClosedEnvironmentComposeProcedure(t *testing.T) {
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	tests := []struct {
		name      string
		operation agentpb.PlanOperation
		marker    string
		want      bool
	}{
		{
			name: "marked full reconcile", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
			marker: string(taskcontract.BlueprintComposeProcedureFullReconcile), want: true,
		},
		{
			name: "unmarked reconcile", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		},
		{
			name: "wrong reconcile marker", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
			marker: string(taskcontract.BlueprintComposeProcedureNone),
		},
		{
			name: "malformed reconcile marker", operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
			marker: "legacy",
		},
		{
			name: "Blueprint none", operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			marker: string(taskcontract.BlueprintComposeProcedureNone), want: true,
		},
		{
			name: "Blueprint candidate Releases", operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			marker: string(taskcontract.BlueprintComposeProcedureCandidateReleases), want: true,
		},
		{
			name: "full reconcile cannot masquerade as Blueprint", operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
			marker: string(taskcontract.BlueprintComposeProcedureFullReconcile),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := map[string]string{}
			if test.marker != "" {
				params[taskcontract.EnvironmentBlueprintProcedureParam] = test.marker
			}
			task := etcd.TaskRecord{Type: testtaskjournal.TaskUpdate, Target: environmentID, Params: params}
			if got := operationMatchesTask(test.operation, task); got != test.want {
				t.Fatalf(
					"operationMatchesTask(%s, marker %q) = %t, want %t",
					test.operation,
					test.marker,
					got,
					test.want,
				)
			}
		})
	}
}
