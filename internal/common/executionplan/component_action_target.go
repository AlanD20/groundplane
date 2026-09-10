package executionplan

import (
	"crypto/subtle"
	"strconv"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateComponentContainerActionTarget(
	operation agentpb.PlanOperation,
	action *agentpb.ComponentApply,
	step *agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
	steps []*agentpb.ExecutionStep,
) error {
	matches := 0
	var selectedArtifact *agentpb.ComposeArtifact
	var selectedService *agentpb.ComposeService
	var blueprintMaterialization *agentpb.MaterializeFile
	if operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		materializations := 0
		for _, candidate := range steps {
			if candidate == nil || candidate.GetStepId() == "" {
				return errs.New(errs.KindValidationFailed, "Blueprint Component prerequisite is missing")
			}
			if materialization := candidate.GetMaterializeFile(); materialization != nil &&
				materialization.GetMaterializationId() == action.GetArtifactId() {
				materializations++
				blueprintMaterialization = materialization
			}
		}
		if materializations != 1 || blueprintMaterialization == nil {
			return errs.New(errs.KindValidationFailed, "Blueprint Component materialization is not unique")
		}
		selectedArtifact = artifacts[blueprintMaterialization.GetArtifactId()]
		if selectedArtifact == nil || selectedArtifact.GetArtifactId() != blueprintMaterialization.GetArtifactId() ||
			selectedArtifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
			selectedArtifact.GetAuthorizedVolumeDir() == "" {
			return errs.New(errs.KindValidationFailed, "Blueprint Component materialization artifact is invalid")
		}
		for _, service := range selectedArtifact.GetServices() {
			if service != nil && service.GetOwnerComponentId() == action.GetComponentId() {
				matches++
				selectedService = service
			}
		}
	} else {
		for _, artifact := range artifacts {
			if artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
				artifact.GetAuthorizedVolumeDir() == "" {
				continue
			}
			for _, service := range artifact.GetServices() {
				if service != nil && service.GetOwnerComponentId() == action.GetComponentId() {
					matches++
					selectedArtifact = artifact
					selectedService = service
				}
			}
		}
	}
	if matches != 1 {
		return errs.New(errs.KindValidationFailed, "Component container action target is not uniquely owned")
	}
	sealedGeneration := ""
	for _, label := range selectedService.GetExpectedLabels() {
		if label.GetKey() == labelRenderGen {
			sealedGeneration = label.GetValue()
			break
		}
	}
	// File generations advance independently of an unchanged serving container.
	// validateLabels authenticates any retained Component runtime ownership.
	runtimeGeneration, generationErr := strconv.ParseUint(sealedGeneration, 10, 64)
	if generationErr != nil || runtimeGeneration == 0 || runtimeGeneration > action.GetGeneration() ||
		strconv.FormatUint(runtimeGeneration, 10) != sealedGeneration ||
		runtimeGeneration < action.GetGeneration() &&
			!retainedComponentReloadTarget(selectedArtifact, selectedService, step, steps) {
		return errs.New(
			errs.KindValidationFailed,
			"Component action generation does not match its selected Service",
		)
	}
	if operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
		if action.GetManagedConfigContent() || len(action.GetExpectedPreviousArtifactDigest()) != 0 ||
			action.GetExpectedPreviousArtifactId() != "" || action.GetExpectedPreviousGeneration() != 0 {
			return errs.New(errs.KindValidationFailed, "Blueprint Component action contains host lifecycle authority")
		}
		positions := make(map[string]int, len(steps))
		materializations := 0
		for index, candidate := range steps {
			if candidate == nil || candidate.GetStepId() == "" {
				return errs.New(errs.KindValidationFailed, "Blueprint Component prerequisite is missing")
			}
			if _, exists := positions[candidate.StepId]; exists {
				return errs.New(errs.KindValidationFailed, "Blueprint Component prerequisite is ambiguous")
			}
			positions[candidate.StepId] = index
			if candidate.GetMaterializeFile().GetMaterializationId() == action.GetArtifactId() {
				materializations++
			}
		}
		position, exists := positions[step.GetStepId()]
		if !exists || materializations != 1 {
			return errs.New(errs.KindValidationFailed, "Blueprint Component materialization is not unique")
		}
		var materialization *agentpb.MaterializeFile
		for predecessor := step.GetPrerequisiteStepId(); predecessor != ""; {
			previous, exists := positions[predecessor]
			if !exists || previous >= position {
				return errs.New(errs.KindValidationFailed, "Blueprint Component prerequisite must precede its consumer")
			}
			candidate := steps[previous]
			switch candidate.GetPolicy() {
			case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK:
				return errs.New(
					errs.KindValidationFailed,
					"Blueprint Component prerequisite contains recovery-only work",
				)
			}
			if candidate.GetMaterializeFile().GetMaterializationId() == action.GetArtifactId() {
				materialization = candidate.GetMaterializeFile()
			}
			if apply := candidate.GetComposeApply(); materialization == nil && apply != nil &&
				apply.GetArtifactId() == selectedArtifact.GetArtifactId() {
				for _, serviceID := range apply.GetServiceIds() {
					if serviceID == selectedService.GetServiceId() &&
						(apply.GetFullReconcile() ||
							!apply.GetNoDependencies() || len(apply.GetServiceIds()) != 1) {
						return errs.New(
							errs.KindValidationFailed,
							"Blueprint Component action prerequisite is not its targeted Compose apply",
						)
					}
				}
			}
			position, predecessor = previous, candidate.GetPrerequisiteStepId()
		}
		if materialization == nil || materialization.GetArtifactId() != selectedArtifact.GetArtifactId() ||
			materialization.GetEnvironmentId() != selectedArtifact.GetOwnerId() ||
			subtle.ConstantTimeCompare(materialization.GetSha256(), action.GetArtifactDigest()) != 1 {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint Component action is not bound to its sealed materialization",
			)
		}
		return nil
	}
	var prerequisite *agentpb.ExecutionStep
	for _, candidate := range steps {
		if candidate.GetStepId() == step.GetPrerequisiteStepId() {
			prerequisite = candidate
			break
		}
	}
	var materialization *agentpb.MaterializeFile
	if prerequisite != nil {
		materialization = prerequisite.GetMaterializeFile()
	}
	if materialization == nil && prerequisite != nil {
		apply := prerequisite.GetComposeApply()
		if apply == nil || apply.GetArtifactId() != selectedArtifact.GetArtifactId() ||
			apply.GetFullReconcile() || !apply.GetNoDependencies() ||
			len(apply.GetServiceIds()) != 1 ||
			apply.GetServiceIds()[0] != selectedService.GetServiceId() {
			return errs.New(
				errs.KindValidationFailed,
				"Component action prerequisite is not its targeted Compose apply",
			)
		}
		for _, candidate := range steps {
			if candidate.GetStepId() == prerequisite.GetPrerequisiteStepId() {
				materialization = candidate.GetMaterializeFile()
				break
			}
		}
	}
	if materialization == nil || materialization.GetMaterializationId() != action.GetArtifactId() ||
		materialization.GetArtifactId() != selectedArtifact.GetArtifactId() ||
		materialization.GetEnvironmentId() != selectedArtifact.GetOwnerId() ||
		subtle.ConstantTimeCompare(materialization.GetSha256(), action.GetArtifactDigest()) != 1 {
		return errs.New(errs.KindValidationFailed, "Component action is not bound to its materialization prerequisite")
	}
	return nil
}
