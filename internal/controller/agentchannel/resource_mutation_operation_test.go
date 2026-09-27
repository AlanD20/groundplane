package agentchannel

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// QA: HTTP-11. Rationale: live Route creation and Volume rename were quarantined
// because their sealed reconcile operations were mistaken for Environment work.
func TestOperationMatchesDirectResourceMutations(t *testing.T) {
	for _, test := range []struct {
		kind, target string
		taskType     testtaskjournal.TaskType
	}{
		{testtaskjournal.TaskResourceVolume, "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", testtaskjournal.TaskUpdate},
		{testtaskjournal.TaskResourceEntry, "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", testtaskjournal.TaskUpdate},
		{testtaskjournal.TaskResourceRoute, "rte_01ARZ3NDEKTSV4RRFFQ69G5FAV", testtaskjournal.TaskCreate},
		{testtaskjournal.TaskResourceRoute, "rte_01ARZ3NDEKTSV4RRFFQ69G5FAV", testtaskjournal.TaskUpdate},
	} {
		t.Run(test.kind+"/"+string(test.taskType), func(t *testing.T) {
			task := etcd.TaskRecord{Type: test.taskType, Target: test.target,
				Params: map[string]string{testtaskjournal.TaskResourceKindParam: test.kind}}
			if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, task) {
				t.Fatal("direct resource mutation rejected its sealed reconcile operation")
			}
			if operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, task) {
				t.Fatal("direct resource mutation accepted Blueprint Apply")
			}
			if operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE, task) {
				t.Fatal("direct resource mutation accepted Environment creation")
			}
			task.Target = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
			if operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, task) {
				t.Fatal("direct resource mutation accepted the wrong target kind")
			}
		})
	}
}
