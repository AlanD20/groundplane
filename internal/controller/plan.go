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
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecutionPlan is what internal/controller/renderer.go ultimately
// produces for one operation — the render plan turned into something
// dispatchable. Both sides of the Agent channel carry plan_id and
// plan_hash; the Agent rejects a bundle whose two projections don't
// match (blueprint.md, "x-gp-execution").
type ExecutionPlan = agentpb.ExecutionPlan

// BuildPlan is the pure step that turns a render plan into an
// ExecutionPlan — TODO: this is where RenderCompose's output, the
// dependency DAG (from x-gp-requires/x-gp-depends_on), and adapter Steps
// (from an attach's ProvisionSteps, a backup's BackupStrategy, …) get
// sequenced and hashed. Pure w.r.t. its inputs, like the rest of the
// renderer (architecture.md, "State translation and materialization").
func BuildPlan(operation, projectID string, renderGeneration int) (*ExecutionPlan, error) {
	return nil, errs.New(errs.KindNotImplemented, "execution plan builder is not implemented")
}
