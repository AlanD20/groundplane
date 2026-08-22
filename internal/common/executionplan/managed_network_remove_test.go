package executionplan

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: Zone deletion crosses the Docker authority boundary, so the
// sealed plan must bind one stable Zone, its Environment, and its id-derived
// physical network name without accepting an artifact or arbitrary command.
func TestManagedNetworkRemovePlanHasClosedIdentity(t *testing.T) {
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	zoneID := ids.NewAt(ids.KindNetwork, now, 1)
	plan := &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: ids.NewAt(ids.KindPlan, now, 2), RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetId: zoneID,
		Steps: []*agentpb.ExecutionStep{{
			StepId: ids.NewAt(ids.KindStep, now, 3), TimeoutSeconds: 120,
			Payload: &agentpb.ExecutionStep_ManagedNetworkRemove{
				ManagedNetworkRemove: &agentpb.ManagedNetworkRemove{
					NetworkId: zoneID, EnvironmentId: ids.NewAt(ids.KindEnvironment, now, 4),
					DockerName: "gp_net_" + strings.ToLower(zoneID),
				},
			},
		}},
	}
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	plan.Steps[0].GetManagedNetworkRemove().DockerName = "operator-selected"
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(arbitrary network name) error = nil")
	}
}
