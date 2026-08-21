// plan.go: the Controller's ONE typed ExecutionPlan — architecture.md,
// "Compose superset and execution boundary": "The Controller creates one
// typed ExecutionPlan and sends an execution bundle containing its
// plan_id and plan_hash, render generation, one canonical Compose file
// per generated project, generated env/file materializations, typed
// steps, expected labels, and dependency ordering." Protobuf carries
// this over the live Agent channel (see proto/agent.proto's
// TaskAssignment.plan_id/plan_hash/render_generation); x-gp-execution is
// the same plan's file-local serialization for inspection/replay.
package controller

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecutionPlan is what internal/controller/renderer.go ultimately
// produces for one operation — the render plan turned into something
// dispatchable. Both sides of the Agent channel carry plan_id and
// plan_hash; the Agent rejects a bundle whose two projections don't
// match (blueprint.md, "x-gp-execution").
type ExecutionPlan = agentpb.ExecutionPlan

// PlanBuildInput is the complete typed output of rendering and procedure
// sequencing. It deliberately contains no persisted rendered-artifact handle:
// a resolver rebuilds these values from retained desired-state inputs.
type PlanBuildInput struct {
	PlanID           string
	RenderGeneration uint64
	Operation        agentpb.PlanOperation
	TargetID         string
	Artifacts        []*agentpb.ComposeArtifact
	Steps            []*agentpb.ExecutionStep
}

// BuildPlan owns schema selection, defensive copying, deterministic hashing,
// and closed-shape validation for every Controller-produced execution plan.
func BuildPlan(input PlanBuildInput) (*ExecutionPlan, error) {
	return executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: 1, PlanId: input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation: input.Operation, TargetId: input.TargetID,
		Artifacts: input.Artifacts, Steps: input.Steps,
	})
}
