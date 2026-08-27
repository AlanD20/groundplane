package coredns

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/core"
)

// TaskStep is the fixed ordering of a CoreDNS component update. Validation and
// rendering happen before the Agent is allowed to reload the live service.
type TaskStep string

const (
	TaskStepValidateConfig TaskStep = "validate_config"
	TaskStepRender         TaskStep = "render_corefile"
	TaskStepApply          TaskStep = "validate_before_reload"
	TaskStepObserve        TaskStep = "observe"
)

// TaskPlan is the deterministic controller-to-Agent artifact for one CoreDNS
// update. Corefile bytes are retained for artifact materialization, while both
// hashes provide stable idempotency/evidence values.
type TaskPlan struct {
	ComponentID    string
	ServiceID      string
	Corefile       []byte
	CorefileSHA256 [32]byte
	InputSHA256    [32]byte
	Steps          []TaskStep
}

// BuildTaskPlan validates the complete component desired state, materializes
// authoritative split-horizon records, renders the candidate, and returns the
// fixed task sequence without any side effects.
func BuildTaskPlan(component core.Component, environments []core.Environment, baseline []ResolverEndpoint) (TaskPlan, error) {
	if err := ValidateComponent(component); err != nil {
		return TaskPlan{}, err
	}
	if !component.Enabled {
		return TaskPlan{}, invalid("coredns: disabled component cannot receive an update plan")
	}
	if len(component.GeneratedServices) != 1 || component.GeneratedServices[0] == "" {
		return TaskPlan{}, invalid("coredns: exactly one generated service is required")
	}
	config, err := DecodeConfig(component.Config)
	if err != nil {
		return TaskPlan{}, err
	}
	input, err := BuildRenderInput(environments, config, baseline)
	if err != nil {
		return TaskPlan{}, err
	}
	corefile, err := RenderCorefile(input)
	if err != nil {
		return TaskPlan{}, err
	}
	inputDigest, err := DigestRenderInput(input)
	if err != nil {
		return TaskPlan{}, err
	}
	return TaskPlan{
		ComponentID:    component.ID,
		ServiceID:      component.GeneratedServices[0],
		Corefile:       append([]byte(nil), corefile...),
		CorefileSHA256: sha256.Sum256(corefile),
		InputSHA256:    inputDigest,
		Steps: []TaskStep{
			TaskStepValidateConfig,
			TaskStepRender,
			TaskStepApply,
			TaskStepObserve,
		},
	}, nil
}

// ValidateComponent enforces the platform ownership and exact desired
// configuration boundary before a plan can be built.
func ValidateComponent(component core.Component) error {
	if component.Kind != core.ComponentKindCoreDNS || component.Owner != core.ComponentOwnerPlatform || component.OwnerID != "" {
		return invalid("coredns: component must be the singleton platform CoreDNS component")
	}
	return ValidateConfig(component.Config)
}

// Validate checks that the Agent receives a complete, internally consistent
// plan rather than a partially materialized candidate.
func (plan TaskPlan) Validate() error {
	if plan.ComponentID == "" || plan.ServiceID == "" || len(plan.Corefile) == 0 || len(plan.Steps) != 4 {
		return invalid("coredns: task plan is incomplete")
	}
	if plan.Steps[0] != TaskStepValidateConfig || plan.Steps[1] != TaskStepRender ||
		plan.Steps[2] != TaskStepApply || plan.Steps[3] != TaskStepObserve {
		return invalid("coredns: task steps are not in deterministic order")
	}
	if sha256.Sum256(plan.Corefile) != plan.CorefileSHA256 {
		return invalid("coredns: task Corefile digest does not match bytes")
	}
	return nil
}

// ObservedState is the immutable projection recorded after the Agent's
// validate-before-reload procedure and health observation.
type ObservedState struct {
	ComponentID         string
	ServiceID           string
	Enabled             bool
	Healthy             bool
	CorefileSHA256      [32]byte
	InputSHA256         [32]byte
	DesiredGeneration   uint64
	RenderGeneration    uint64
	AgentID             string
	AgentGeneration     uint64
	BaselineGeneration  uint64
	OwnershipGeneration uint64
}

// Observe constructs the state projection only after a plan has been checked.
func Observe(plan TaskPlan, healthy bool) (ObservedState, error) {
	if err := plan.Validate(); err != nil {
		return ObservedState{}, err
	}
	return ObservedState{
		ComponentID:    plan.ComponentID,
		ServiceID:      plan.ServiceID,
		Enabled:        true,
		Healthy:        healthy,
		CorefileSHA256: plan.CorefileSHA256,
		InputSHA256:    plan.InputSHA256,
	}, nil
}
