package executionplan

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"strings"
)

func validateStep(
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
	artifacts map[string]*agentpb.ComposeArtifact,
	steps []*agentpb.ExecutionStep,
) error {
	operation, renderGeneration := plan.GetOperation(), plan.GetRenderGeneration()
	if step == nil || validateID(ids.KindStep, step.StepId) != nil || step.TimeoutSeconds == 0 {
		return errs.New(errs.KindValidationFailed, "execution step identity or timeout is invalid")
	}
	switch payload := step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		return validateComposeApply(plan, step, artifacts)
	case *agentpb.ExecutionStep_ComposeStop:
		if payload.ComposeStop == nil || payload.ComposeStop.GraceSeconds == 0 ||
			payload.ComposeStop.GraceSeconds > 300 {
			return errs.New(errs.KindValidationFailed, "Compose stop payload is invalid")
		}
		return validateSelection(payload.ComposeStop.ArtifactId, payload.ComposeStop.ServiceIds, artifacts, true)
	case *agentpb.ExecutionStep_ComposeRemove:
		if payload.ComposeRemove == nil || payload.ComposeRemove.WholeProject ==
			(len(payload.ComposeRemove.ServiceIds) != 0) {
			return errs.New(errs.KindValidationFailed, "Compose remove selection is inconsistent")
		}
		if payload.ComposeRemove.WholeProject && operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE {
			return errs.New(errs.KindValidationFailed, "whole-project removal requires a remove operation")
		}
		return validateSelection(
			payload.ComposeRemove.ArtifactId,
			payload.ComposeRemove.ServiceIds,
			artifacts,
			!payload.ComposeRemove.WholeProject,
		)
	case *agentpb.ExecutionStep_WaitHealthy:
		if payload.WaitHealthy == nil {
			return errs.New(errs.KindValidationFailed, "health wait payload is empty")
		}
		return validateSelection(payload.WaitHealthy.ArtifactId, payload.WaitHealthy.ServiceIds, artifacts, true)
	case *agentpb.ExecutionStep_ComposeWorkloadApply:
		return validateReleaseWorkloadStep(
			operation,
			payload.ComposeWorkloadApply.GetArtifactId(),
			payload.ComposeWorkloadApply.GetServiceId(),
			payload.ComposeWorkloadApply.GetTarget(),
			artifacts,
		)
	case *agentpb.ExecutionStep_WaitWorkloadHealthy:
		return validateReleaseWorkloadStep(
			operation,
			payload.WaitWorkloadHealthy.GetArtifactId(),
			payload.WaitWorkloadHealthy.GetServiceId(),
			payload.WaitWorkloadHealthy.GetTarget(),
			artifacts,
		)
	case *agentpb.ExecutionStep_ServiceProxySwitch:
		return validateServiceProxySwitch(operation, payload.ServiceProxySwitch, artifacts)
	case *agentpb.ExecutionStep_ServiceProxyProbe:
		return validateServiceProxyProbe(operation, payload.ServiceProxyProbe, artifacts)
	case *agentpb.ExecutionStep_ServiceProxyCompensate:
		return validateServiceProxyCompensate(operation, payload.ServiceProxyCompensate, artifacts)
	case *agentpb.ExecutionStep_ServiceRecreateAcknowledge:
		return validateServiceRecreateAcknowledge(operation, payload.ServiceRecreateAcknowledge, artifacts)
	case *agentpb.ExecutionStep_ServiceRecreateProbe:
		return validateServiceRecreateProbe(operation, payload.ServiceRecreateProbe, artifacts)
	case *agentpb.ExecutionStep_ServiceRecreateCompensate:
		return validateServiceRecreateCompensate(operation, payload.ServiceRecreateCompensate, artifacts)
	case *agentpb.ExecutionStep_CandidateRestorationProbe:
		value := payload.CandidateRestorationProbe
		return validateCandidateRestorationStep(operation, value.GetCandidateArtifactId(), value.GetServiceId(), value.GetCandidateReleaseId(), artifacts)
	case *agentpb.ExecutionStep_CandidateRestorationCompensate:
		value := payload.CandidateRestorationCompensate
		return validateCandidateRestorationStep(operation, value.GetCandidateArtifactId(), value.GetServiceId(), value.GetCandidateReleaseId(), artifacts)
	case *agentpb.ExecutionStep_EnvironmentDirectoryCreate:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE ||
			payload.EnvironmentDirectoryCreate == nil ||
			validateID(ids.KindEnvironment, payload.EnvironmentDirectoryCreate.EnvironmentId) != nil {
			return errs.New(errs.KindValidationFailed, "environment directory create payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_EnvironmentDirectoryRemove:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE ||
			payload.EnvironmentDirectoryRemove == nil ||
			validateID(ids.KindEnvironment, payload.EnvironmentDirectoryRemove.EnvironmentId) != nil {
			return errs.New(errs.KindValidationFailed, "environment directory remove payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure:
		return validateManagedVolumeDirectoriesEnsure(operation, payload.ManagedVolumeDirectoriesEnsure, artifacts)
	case *agentpb.ExecutionStep_ManagedVolumeEnsure:
		return validateManagedVolumeEnsure(plan, payload.ManagedVolumeEnsure, artifacts)
	case *agentpb.ExecutionStep_ManagedVolumeRemove:
		remove := payload.ManagedVolumeRemove
		if operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || remove == nil ||
			validateID(ids.KindVolume, remove.VolumeId) != nil ||
			remove.DockerName != "gp_vol_"+strings.ToLower(remove.VolumeId) {
			return errs.New(errs.KindValidationFailed, "managed Volume remove payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_ManagedVolumeDirectoryRemove:
		remove := payload.ManagedVolumeDirectoryRemove
		if operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || remove == nil ||
			validateID(ids.KindVolume, remove.VolumeId) != nil || !validManagedVolumeComposeKey(remove.ComposeKey) ||
			len(remove.IntentSha256) != sha256.Size {
			return errs.New(errs.KindValidationFailed, "managed Volume directory remove payload is invalid")
		}
		artifact := artifacts[remove.ArtifactId]
		if artifact == nil || artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT {
			return errs.New(errs.KindValidationFailed, "managed Volume directory remove artifact is invalid")
		}
		for _, volume := range artifact.Volumes {
			if volume != nil && volume.VolumeId == remove.VolumeId && volume.ComposeName == remove.ComposeKey {
				return nil
			}
		}
		return errs.New(errs.KindValidationFailed, "managed Volume directory remove volume is not in its artifact")
	case *agentpb.ExecutionStep_MaterializeFile:
		return validateMaterializeFile(renderGeneration, payload.MaterializeFile, artifacts)
	case *agentpb.ExecutionStep_AdapterProcedure:
		return validateAdapterProcedure(operation, payload.AdapterProcedure)
	case *agentpb.ExecutionStep_BackingHookProcedure:
		return validateBackingHookProcedure(operation, payload.BackingHookProcedure)
	case *agentpb.ExecutionStep_ManagedNetworkEnsure:
		return validateManagedNetworkEnsure(operation, payload.ManagedNetworkEnsure, artifacts)
	case *agentpb.ExecutionStep_ManagedNetworkRemove:
		remove := payload.ManagedNetworkRemove
		if operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE || remove == nil {
			return errs.New(errs.KindValidationFailed, "managed network remove payload is invalid")
		}
		dockerName, err := networkname.New(remove.NetworkId)
		if err != nil ||
			validateID(ids.KindEnvironment, remove.EnvironmentId) != nil ||
			remove.DockerName != dockerName {
			return errs.New(errs.KindValidationFailed, "managed network remove payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_ComponentApply:
		apply := payload.ComponentApply
		if apply == nil || (operation != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE) {
			return errs.New(errs.KindValidationFailed, "component apply payload is invalid")
		}
		if err := validateComponentAction(apply); err != nil {
			return err
		}
		if operation == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
			return nil
		}
		if apply.GetGeneration() != renderGeneration {
			return errs.New(errs.KindValidationFailed, "Component action generation does not match its plan")
		}
		return validateComponentContainerActionTarget(operation, apply, step, artifacts, steps)
	case *agentpb.ExecutionStep_HostResolutionApply:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY ||
			!validHostResolutionAction(payload.HostResolutionApply.GetComponentId(),
				payload.HostResolutionApply.GetGeneration(), renderGeneration) {
			return errs.New(errs.KindValidationFailed, "host resolution apply payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_HostResolutionRestore:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY ||
			!validHostResolutionAction(payload.HostResolutionRestore.GetComponentId(),
				payload.HostResolutionRestore.GetGeneration(), renderGeneration) {
			return errs.New(errs.KindValidationFailed, "host resolution restore payload is invalid")
		}
		return nil
	case *agentpb.ExecutionStep_RunScript:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_DEPLOY &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK &&
			operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
			return errs.New(errs.KindValidationFailed, "Script step requires release or Blueprint reconcile authority")
		}
		return nil
	case *agentpb.ExecutionStep_BackupArtifactPrune:
		if operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
			return errs.New(errs.KindValidationFailed, "backup artifact prune requires a backup prune operation")
		}
		return validateBackupArtifactPrune("", payload.BackupArtifactPrune.GetOrdinal(), payload.BackupArtifactPrune)
	default:
		return errs.New(errs.KindValidationFailed, "execution step payload is unsupported")
	}
}
