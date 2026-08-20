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
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ExecutionPlan is what internal/controller/renderer.go ultimately
// produces for one operation — the render plan turned into something
// dispatchable. Both sides of the Agent channel carry plan_id and
// plan_hash; the Agent rejects a bundle whose two projections don't
// match (blueprint.md, "x-gp-execution").
type ExecutionPlan struct {
	PlanID           string          `json:"plan_id"`
	PlanHash         string          `json:"plan_hash"` // sha256 of the plan's canonical serialization
	RenderGeneration int             `json:"render_generation"`
	Operation        string          `json:"operation"` // e.g. "reconcile", "deploy", "backup"
	ProjectID        string          `json:"project_id"`
	Steps            []ExecutionStep `json:"steps"`
}

// ExecutionStep is one member of the plan — a compose-level action
// (compose_up, wait_healthy, …) or an adapters.Step (sql, dump, …).
// Kept as two separate optional fields rather than a single
// interface{}/any payload, per standards.md's banned-patterns list (no
// `any` for model fields).
type ExecutionStep struct {
	ID          string         `json:"id"`
	Kind        string         `json:"kind"` // "compose_up" | "wait_healthy" | "adapter_step" | …
	Services    []string       `json:"services,omitempty"`
	Service     string         `json:"service,omitempty"`
	AdapterStep *adapters.Step `json:"adapter_step,omitempty"`
}

// BuildPlan is the pure step that turns a render plan into an
// ExecutionPlan — TODO: this is where RenderCompose's output, the
// dependency DAG (from x-gp-requires/x-gp-depends_on), and adapter Steps
// (from an attach's ProvisionSteps, a backup's BackupStrategy, …) get
// sequenced and hashed. Pure w.r.t. its inputs, like the rest of the
// renderer (architecture.md, "State translation and materialization").
func BuildPlan(operation, projectID string, renderGeneration int) (ExecutionPlan, error) {
	return ExecutionPlan{}, errs.New(errs.KindNotImplemented, "execution plan builder is not implemented")
}
