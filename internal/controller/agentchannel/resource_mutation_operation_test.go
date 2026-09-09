package agentchannel

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a real QA Volume rename was quarantined after claim because its
// read-only reconcile plan was mistaken for an unmarked Environment reconcile.
func TestOperationMatchesDirectResourceMutations(t *testing.T) {
	for _, test := range []struct{ kind, target string }{
		{etcd.TaskResourceVolume, "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		{etcd.TaskResourceEntry, "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	} {
		t.Run(test.kind, func(t *testing.T) {
			task := etcd.TaskRecord{Type: etcd.TaskUpdate, Target: test.target,
				Params: map[string]string{etcd.TaskResourceKindParam: test.kind}}
			if !operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, task) {
				t.Fatal("direct resource mutation rejected its sealed reconcile operation")
			}
			if operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, task) {
				t.Fatal("direct resource mutation accepted Blueprint Apply")
			}
			task.Target = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
			if operationMatchesTask(agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, task) {
				t.Fatal("direct resource mutation accepted the wrong target kind")
			}
		})
	}
}
