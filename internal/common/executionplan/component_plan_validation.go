package executionplan

import (
	"bytes"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"strconv"
)

func componentArtifactValidationPlan(
	plan *agentpb.ExecutionPlan,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ExecutionPlan, error) {
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY ||
		plan.GetComponentLifecycleMode() != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE ||
		len(plan.GetArtifacts()) != 2 || artifact == nil {
		return plan, nil
	}
	candidateArtifactID := ""
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil {
			candidateArtifactID = apply.GetArtifactId()
			break
		}
	}
	if artifact.GetArtifactId() == candidateArtifactID {
		return plan, nil
	}
	if len(artifact.GetServices()) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "Component rollback artifact is invalid")
	}
	rollbackPlanID := ""
	rollbackGeneration := uint64(0)
	for _, label := range artifact.GetServices()[0].GetExpectedLabels() {
		switch label.GetKey() {
		case labelPlanID:
			rollbackPlanID = label.GetValue()
		case labelRenderGen:
			rollbackGeneration, _ = strconv.ParseUint(label.GetValue(), 10, 64)
		}
	}
	if validateID(ids.KindPlan, rollbackPlanID) != nil || rollbackGeneration == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Component rollback artifact ownership is invalid")
	}
	validationPlan := proto.CloneOf(plan)
	validationPlan.PlanId = rollbackPlanID
	validationPlan.RenderGeneration = rollbackGeneration
	return validationPlan, nil
}

// validateComponentApplyPlan keeps the platform component procedure closed:
// one CoreDNS payload is the only step, and an enabled apply must name the
// exact platform artifact and generated service it was rendered from.
func validateComponentApplyPlan(plan *agentpb.ExecutionPlan, artifacts map[string]*agentpb.ComposeArtifact) error {
	if len(plan.Steps) != 2 && len(plan.Steps) != 4 {
		return errs.New(errs.KindValidationFailed, "component apply plan must contain two or four steps")
	}
	for _, step := range plan.Steps {
		if err := validateStep(plan, step, artifacts, plan.Steps); err != nil {
			return err
		}
	}
	step := plan.Steps[0]
	component := step.GetComponentApply()
	if component == nil {
		if len(plan.Steps) == 2 &&
			plan.GetComponentLifecycleMode() == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE {
			restore := step.GetHostResolutionRestore()
			remove := plan.Steps[1].GetComposeRemove()
			rollback := plan.GetComponentRollbackObservation()
			if restore == nil || remove == nil || len(artifacts) != 1 ||
				rollback == nil || rollback.GetManagedConfigContent() || validateComponentAction(rollback) != nil ||
				rollback.GetComponentId() != plan.TargetId || rollback.GetGeneration() != plan.RenderGeneration ||
				restore.GetComponentId() != plan.TargetId || restore.GetGeneration() != plan.RenderGeneration ||
				step.GetPrerequisiteStepId() != "" ||
				plan.Steps[1].GetPrerequisiteStepId() != step.GetStepId() ||
				remove.GetWholeProject() || len(remove.GetServiceIds()) != 1 {
				return errs.New(errs.KindValidationFailed, "component disable procedure is invalid")
			}
			return nil
		}
		return errs.New(errs.KindValidationFailed, "component apply plan must contain a ComponentApply step")
	}
	if plan.GetComponentRollbackObservation() != nil {
		return errs.New(errs.KindValidationFailed, "enabled Component plan carries a rollback observation")
	}
	if err := validateComponentAction(component); err != nil || !component.GetManagedConfigContent() {
		return err
	}
	if component.GetComponentId() != plan.TargetId || component.GetGeneration() != plan.RenderGeneration {
		return errs.New(errs.KindValidationFailed, "component action identity does not match the execution plan")
	}
	if len(plan.Steps) == 2 {
		observation := plan.Steps[1].GetComponentApply()
		if plan.GetComponentLifecycleMode() != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE ||
			len(artifacts) != 1 || observation == nil || observation.GetManagedConfigContent() ||
			plan.Steps[1].GetPrerequisiteStepId() != plan.Steps[0].GetStepId() ||
			validateComponentAction(observation) != nil || observation.GetComponentId() != component.GetComponentId() ||
			observation.GetGeneration() != component.GetGeneration() ||
			!bytes.Equal(observation.GetDefinitionDigest(), component.GetDefinitionDigest()) ||
			!bytes.Equal(observation.GetCatalogDigest(), component.GetCatalogDigest()) ||
			observation.GetArtifactId() != component.GetArtifactId() ||
			!bytes.Equal(observation.GetArtifactDigest(), component.GetArtifactDigest()) ||
			observation.GetActionId() == component.GetActionId() {
			return errs.New(errs.KindValidationFailed, "component reload observation procedure is invalid")
		}
		artifact := onlyComposeArtifact(artifacts)
		if artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM ||
			len(artifact.GetServices()) != 1 || artifact.GetServices()[0].GetHasHealthcheck() {
			return errs.New(errs.KindValidationFailed, "component reload observation artifact is invalid")
		}
		return nil
	}
	apply := plan.Steps[1].GetComposeApply()
	observation := plan.Steps[2].GetComponentApply()
	hostResolution := plan.Steps[3].GetHostResolutionApply()
	if apply == nil {
		return errs.New(errs.KindValidationFailed, "component Service ensure procedure is invalid")
	}
	lifecycleMode := plan.GetComponentLifecycleMode()
	candidateArtifact := artifacts[apply.GetArtifactId()]
	var rollbackArtifact *agentpb.ComposeArtifact
	for artifactID, artifact := range artifacts {
		if artifactID != apply.GetArtifactId() {
			rollbackArtifact = artifact
		}
	}
	validLifecycle := lifecycleMode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE &&
		len(artifacts) == 1 ||
		lifecycleMode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE &&
			len(artifacts) == 2 && len(component.GetExpectedPreviousArtifactDigest()) == sha256.Size
	if !validLifecycle ||
		candidateArtifact == nil || len(candidateArtifact.GetServices()) != 1 ||
		apply == nil || observation == nil || observation.GetManagedConfigContent() ||
		validateComponentAction(observation) != nil ||
		plan.Steps[1].GetPrerequisiteStepId() != plan.Steps[0].GetStepId() ||
		plan.Steps[2].GetPrerequisiteStepId() != plan.Steps[1].GetStepId() ||
		len(apply.GetServiceIds()) != 1 || observation.GetComponentId() != component.GetComponentId() ||
		observation.GetGeneration() != component.GetGeneration() || observation.GetActionId() == component.GetActionId() ||
		observation.GetArtifactId() != component.GetArtifactId() ||
		!bytes.Equal(observation.GetArtifactDigest(), component.GetArtifactDigest()) ||
		hostResolution == nil || plan.Steps[3].GetPrerequisiteStepId() != plan.Steps[2].GetStepId() ||
		hostResolution.GetComponentId() != plan.TargetId ||
		hostResolution.GetGeneration() != plan.RenderGeneration ||
		(rollbackArtifact != nil && (len(rollbackArtifact.GetServices()) != 1 ||
			rollbackArtifact.GetServices()[0].GetServiceId() != candidateArtifact.GetServices()[0].GetServiceId() ||
			rollbackArtifact.GetServices()[0].GetComposeName() != candidateArtifact.GetServices()[0].GetComposeName() ||
			rollbackArtifact.GetServices()[0].GetOwnerComponentId() != candidateArtifact.GetServices()[0].GetOwnerComponentId() ||
			rollbackArtifact.GetServices()[0].GetHasHealthcheck())) {
		return errs.New(errs.KindValidationFailed, "component Service ensure procedure is invalid")
	}
	return nil
}

func onlyComposeArtifact(artifacts map[string]*agentpb.ComposeArtifact) *agentpb.ComposeArtifact {
	for _, artifact := range artifacts {
		return artifact
	}
	return nil
}

func validHostResolutionAction(componentID string, generation uint64, expectedGeneration uint64) bool {
	return validateID(ids.KindComponent, componentID) == nil && generation != 0 && generation == expectedGeneration
}

func validateComponentAction(action *agentpb.ComponentApply) error {
	if action == nil || validateID(ids.KindComponent, action.GetComponentId()) != nil ||
		validateID(ids.KindConfig, action.GetArtifactId()) != nil ||
		!validComponentActionID(action.GetActionId()) || action.GetGeneration() == 0 ||
		!validComponentDigest(action.GetDefinitionDigest()) ||
		!validComponentDigest(action.GetCatalogDigest()) ||
		!validComponentDigest(action.GetArtifactDigest()) ||
		(len(action.GetExpectedPreviousArtifactDigest()) != 0 &&
			!validComponentDigest(action.GetExpectedPreviousArtifactDigest())) ||
		(len(action.GetExpectedPreviousArtifactDigest()) == 0) != (action.GetExpectedPreviousArtifactId() == "") ||
		(len(action.GetExpectedPreviousArtifactDigest()) == 0) != (action.GetExpectedPreviousGeneration() == 0) ||
		(action.GetExpectedPreviousArtifactId() != "" &&
			validateID(ids.KindConfig, action.GetExpectedPreviousArtifactId()) != nil) ||
		(len(action.GetExpectedPreviousArtifactDigest()) != 0 && !action.GetManagedConfigContent()) {
		return errs.New(errs.KindValidationFailed, "component action identity, generation, or digest is invalid")
	}
	return nil
}

func validComponentActionID(value string) bool {
	if value == "" || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, r := range value {
		if !(r == '-' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func validComponentDigest(value []byte) bool {
	if len(value) != sha256.Size {
		return false
	}
	var nonzero byte
	for _, part := range value {
		nonzero |= part
	}
	return nonzero != 0
}
