package controller

import (
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestServiceLifecycleProcedureUsesTargetedComposeOperations(t *testing.T) {
	// Rationale: Start, Stop, and Destroy intentionally preserve different
	// Docker resources and must never collapse into a full reconcile/remove.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 1)
	artifactID := ids.NewAt(ids.KindConfig, at, 2)
	for _, test := range []struct {
		taskType  etcd.TaskType
		operation agentpb.PlanOperation
		assert    func(*testing.T, *agentpb.ExecutionStep)
	}{
		{taskType: etcd.TaskStart, operation: agentpb.PlanOperation_PLAN_OPERATION_START, assert: func(t *testing.T, step *agentpb.ExecutionStep) {
			if apply := step.GetComposeApply(); apply == nil || apply.FullReconcile || len(apply.ServiceIds) != 1 || apply.ServiceIds[0] != serviceID {
				t.Fatalf("Start step = %#v", step)
			}
		}},
		{taskType: etcd.TaskStop, operation: agentpb.PlanOperation_PLAN_OPERATION_STOP, assert: func(t *testing.T, step *agentpb.ExecutionStep) {
			if stop := step.GetComposeStop(); stop == nil || stop.GraceSeconds != ServiceStopGraceSeconds || len(stop.ServiceIds) != 1 || stop.ServiceIds[0] != serviceID {
				t.Fatalf("Stop step = %#v", step)
			}
		}},
		{taskType: etcd.TaskDestroy, operation: agentpb.PlanOperation_PLAN_OPERATION_DESTROY, assert: func(t *testing.T, step *agentpb.ExecutionStep) {
			if remove := step.GetComposeRemove(); remove == nil || remove.WholeProject || len(remove.ServiceIds) != 1 || remove.ServiceIds[0] != serviceID {
				t.Fatalf("Destroy step = %#v", step)
			}
		}},
	} {
		task := etcd.TaskRecord{
			Type: test.taskType, Target: serviceID, TimeoutSeconds: 120,
			Steps: []etcd.TaskStepRecord{{ID: ids.New(ids.KindStep)}},
		}
		operation, step, err := serviceLifecycleProcedure(task, artifactID)
		if err != nil || operation != test.operation || step.TimeoutSeconds != 120 {
			t.Fatalf("serviceLifecycleProcedure(%s) = %v, %#v, %v", test.taskType, operation, step, err)
		}
		test.assert(t, step)
	}
}
