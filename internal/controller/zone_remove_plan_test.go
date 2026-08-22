package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: a durable Zone Task must reconstruct the same closed helper
// procedure after Controller restart using only stable Task inputs.
func TestZoneRemovalPlanRebuildsManagedNetworkProcedure(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	zoneID := ids.NewAt(ids.KindNetwork, now, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2)
	task := etcd.TaskRecord{
		ID: ids.NewAt(ids.KindTask, now, 3), OperationID: ids.NewAt(ids.KindOperation, now, 4),
		Executor: etcd.TaskExecutorAgent, PlanID: ids.NewAt(ids.KindPlan, now, 5), RenderGeneration: 1,
		Type: etcd.TaskRemove, Target: zoneID,
		Params: map[string]string{etcd.TaskZoneEnvironmentParam: environmentID},
		Steps:  []etcd.TaskStepRecord{{ID: ids.NewAt(ids.KindStep, now, 6)}}, TimeoutSeconds: 120,
		Status: etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now,
	}
	resolver, err := NewTaskPlanResolver("/var/lib/groundplane/vol")
	if err != nil {
		t.Fatalf("NewTaskPlanResolver() error = %v", err)
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	remove := plan.Steps[0].GetManagedNetworkRemove()
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || plan.TargetId != zoneID ||
		len(plan.Artifacts) != 0 || remove == nil || remove.NetworkId != zoneID ||
		remove.EnvironmentId != environmentID || remove.DockerName != "gp_net_"+strings.ToLower(zoneID) {
		t.Fatalf("Zone removal plan = %#v", plan)
	}
}
