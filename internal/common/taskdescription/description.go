// Package taskdescription projects sealed Agent execution steps into safe,
// operator-facing descriptions.
package taskdescription

import (
	"fmt"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Description is the safe operator-facing projection of one execution step.
type Description struct {
	Action         string
	Description    string
	Target         string
	TimeoutSeconds uint32
}

// Describe returns a safe operator-facing description of step. It deliberately
// excludes commands, content, credentials, paths and arbitrary execution output.
func Describe(step *agentpb.ExecutionStep) Description {
	description := Description{
		Action:      "Execution details unavailable",
		Description: "The sealed plan has no supported operator-safe details for this step.",
	}
	if step == nil {
		return description
	}
	description.TimeoutSeconds = step.GetTimeoutSeconds()

	switch payload := step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		return projected(
			description,
			"Apply container configuration",
			"Apply the sealed container configuration for "+selectedResources(len(payload.ComposeApply.GetServiceIds()), "Service", "Services")+".",
			singleTarget(payload.ComposeApply.GetServiceIds()),
		)
	case *agentpb.ExecutionStep_ComposeStop:
		return projected(
			description,
			"Stop workload containers",
			"Stop containers for "+selectedResources(len(payload.ComposeStop.GetServiceIds()), "Service", "Services")+" before the next planned change.",
			singleTarget(payload.ComposeStop.GetServiceIds()),
		)
	case *agentpb.ExecutionStep_ComposeRemove:
		return projected(
			description,
			"Remove workload containers",
			composeRemoveDescription(len(payload.ComposeRemove.GetServiceIds())),
			singleTarget(payload.ComposeRemove.GetServiceIds()),
		)
	case *agentpb.ExecutionStep_WaitHealthy:
		return projected(
			description,
			"Wait for workload health",
			"Wait for "+selectedResources(len(payload.WaitHealthy.GetServiceIds()), "Service", "Services")+" to pass their configured health checks.",
			singleTarget(payload.WaitHealthy.GetServiceIds()),
		)
	case *agentpb.ExecutionStep_EnvironmentDirectoryCreate:
		return projected(
			description,
			"Create Environment directory",
			"Prepare managed storage for the selected Environment.",
			payload.EnvironmentDirectoryCreate.GetEnvironmentId(),
		)
	case *agentpb.ExecutionStep_MaterializeFile:
		return projected(
			description,
			"Update managed configuration file",
			"Reconcile one managed configuration output from the sealed plan.",
			firstTarget(payload.MaterializeFile.GetServiceId(), payload.MaterializeFile.GetEnvironmentId()),
		)
	case *agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure:
		return projected(
			description,
			"Prepare Volume directories",
			"Prepare managed storage directories for "+selectedResources(len(payload.ManagedVolumeDirectoriesEnsure.GetVolumeIds()), "Volume", "Volumes")+".",
			singleTarget(payload.ManagedVolumeDirectoriesEnsure.GetVolumeIds()),
		)
	case *agentpb.ExecutionStep_EnvironmentDirectoryRemove:
		return projected(
			description,
			"Remove Environment directory",
			"Remove managed storage for the selected Environment.",
			payload.EnvironmentDirectoryRemove.GetEnvironmentId(),
		)
	case *agentpb.ExecutionStep_AdapterProcedure:
		return describeAdapter(description, payload.AdapterProcedure)
	case *agentpb.ExecutionStep_ManagedNetworkRemove:
		return projected(
			description,
			"Remove Zone network",
			"Remove the managed network for the selected Zone after its consumers are detached.",
			payload.ManagedNetworkRemove.GetNetworkId(),
		)
	case *agentpb.ExecutionStep_ComposeWorkloadApply:
		return projected(
			description,
			"Start workload containers",
			"Start the candidate workload containers for the selected Service.",
			payload.ComposeWorkloadApply.GetServiceId(),
		)
	case *agentpb.ExecutionStep_WaitWorkloadHealthy:
		return projected(
			description,
			"Wait for workload health",
			"Wait for the candidate workload of the selected Service to pass its configured health check.",
			payload.WaitWorkloadHealthy.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ServiceProxySwitch:
		return projected(
			description,
			"Switch Service proxy to deployed workload",
			"Direct the selected Service proxy to its candidate workload.",
			payload.ServiceProxySwitch.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ServiceProxyProbe:
		return projected(
			description,
			"Verify Service proxy traffic",
			"Verify that the selected Service proxy is using the planned workload.",
			payload.ServiceProxyProbe.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ServiceProxyCompensate:
		return projected(
			description,
			"Restore previous Service proxy",
			"Return the selected Service proxy to its previous workload after a failed change.",
			payload.ServiceProxyCompensate.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ServiceRecreateAcknowledge:
		return projected(
			description,
			"Record deployed Service runtime",
			"Record the recreated runtime as current for the selected Service.",
			payload.ServiceRecreateAcknowledge.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ServiceRecreateProbe:
		return projected(
			description,
			"Verify recreated Service runtime",
			"Check whether the selected Service needs its previous runtime restored.",
			payload.ServiceRecreateProbe.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ServiceRecreateCompensate:
		return projected(
			description,
			"Restore previous Service runtime",
			"Restore the previous runtime for the selected Service after a failed recreation.",
			payload.ServiceRecreateCompensate.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ManagedVolumeRemove:
		return projected(
			description,
			"Remove managed Volume",
			"Remove the selected managed Volume after its consumers are detached.",
			payload.ManagedVolumeRemove.GetVolumeId(),
		)
	case *agentpb.ExecutionStep_ManagedVolumeDirectoryRemove:
		return projected(
			description,
			"Remove Volume directory",
			"Remove managed host storage for the selected Volume.",
			payload.ManagedVolumeDirectoryRemove.GetVolumeId(),
		)
	case *agentpb.ExecutionStep_ComponentApply:
		return projected(
			description,
			"Apply Component configuration",
			"Apply the sealed catalog action for the selected Component.",
			payload.ComponentApply.GetComponentId(),
		)
	case *agentpb.ExecutionStep_HostResolutionApply:
		return projected(
			description,
			"Apply host DNS configuration",
			"Publish host DNS resolution managed by the selected Component.",
			payload.HostResolutionApply.GetComponentId(),
		)
	case *agentpb.ExecutionStep_HostResolutionRestore:
		return projected(
			description,
			"Restore host DNS configuration",
			"Restore the previous host DNS resolution managed by the selected Component.",
			payload.HostResolutionRestore.GetComponentId(),
		)
	case *agentpb.ExecutionStep_RunScript:
		return projected(
			description,
			"Run configured script",
			"Run the selected Script with its sealed runner configuration.",
			payload.RunScript.GetScriptId(),
		)
	case *agentpb.ExecutionStep_CandidateRestorationProbe:
		return projected(
			description,
			"Verify previous workload restoration",
			"Check whether the selected Service needs its previous workload restored.",
			payload.CandidateRestorationProbe.GetServiceId(),
		)
	case *agentpb.ExecutionStep_CandidateRestorationCompensate:
		return projected(
			description,
			"Restore previous workload",
			"Restore the previous workload for the selected Service after a failed change.",
			payload.CandidateRestorationCompensate.GetServiceId(),
		)
	case *agentpb.ExecutionStep_ManagedNetworkEnsure:
		return projected(
			description,
			"Prepare Zone network",
			"Prepare the managed network for the selected Zone.",
			payload.ManagedNetworkEnsure.GetNetworkId(),
		)
	case *agentpb.ExecutionStep_ManagedVolumeEnsure:
		return projected(
			description,
			"Prepare managed Volume",
			"Prepare the selected managed Volume for its consumers.",
			payload.ManagedVolumeEnsure.GetVolumeId(),
		)
	case *agentpb.ExecutionStep_BackingHookProcedure:
		return describeBackingHook(description, payload.BackingHookProcedure)
	case *agentpb.ExecutionStep_BackupStep:
		return describeBackup(description, payload.BackupStep)
	default:
		return description
	}
}

func projected(base Description, action, detail, target string) Description {
	base.Action = action
	base.Description = detail
	base.Target = target
	return base
}

func selectedResources(count int, singular, plural string) string {
	switch count {
	case 0:
		return "the selected " + plural
	case 1:
		return "the selected " + singular
	default:
		return fmt.Sprintf("%d selected %s", count, plural)
	}
}

func singleTarget(targets []string) string {
	if len(targets) == 1 {
		return targets[0]
	}
	return ""
}

func firstTarget(targets ...string) string {
	for _, target := range targets {
		if target != "" {
			return target
		}
	}
	return ""
}

func composeRemoveDescription(serviceCount int) string {
	if serviceCount == 0 {
		return "Remove all workload containers selected by the sealed plan."
	}
	return "Remove containers for " + selectedResources(serviceCount, "Service", "Services") + "."
}

func describeAdapter(base Description, procedure *agentpb.AdapterProcedure) Description {
	phase := procedure.GetPhase()
	adapter := procedure.GetAdapterKey()
	action := "Run backing adapter procedure"
	if knownPhase := adapterPhaseName(phase); knownPhase != "" {
		action = "Run backing adapter: " + knownPhase
		if adapter != "" {
			action = "Run " + adapter + " adapter: " + knownPhase
		}
	}

	detail := "Run the sealed procedure using the selected backing adapter."
	target := firstTarget(procedure.GetAttachId(), procedure.GetBackingServiceId())
	switch phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION:
		detail = adapterPurpose(adapter, "provision the selected Backing Service")
		target = procedure.GetBackingServiceId()
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT:
		detail = adapterPurpose(adapter, "grant the selected Attach access to its Backing Service")
		target = firstTarget(procedure.GetAttachId(), procedure.GetBackingServiceId())
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		detail = adapterPurpose(adapter, "detach the selected Attach from its Backing Service")
		target = firstTarget(procedure.GetAttachId(), procedure.GetBackingServiceId())
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
		detail = adapterPurpose(adapter, "revoke the selected Attach access to its Backing Service")
		target = firstTarget(procedure.GetAttachId(), procedure.GetBackingServiceId())
	}
	return projected(base, action, detail, target)
}

func adapterPhaseName(phase agentpb.AdapterProcedurePhase) string {
	switch phase {
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_PROVISION:
		return "provision"
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_GRANT:
		return "grant"
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_DETACH:
		return "detach"
	case agentpb.AdapterProcedurePhase_ADAPTER_PROCEDURE_PHASE_REVOKE:
		return "revoke"
	default:
		return ""
	}
}

func adapterPurpose(adapter, purpose string) string {
	if adapter == "" {
		return "Use the selected backing adapter to " + purpose + "."
	}
	return "Use the " + adapter + " adapter to " + purpose + "."
}

func describeBackingHook(base Description, procedure *agentpb.BackingHookProcedure) Description {
	switch procedure.GetEvent() {
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_ATTACH:
		return projected(
			base,
			"Run backing hook: attach",
			"Run the configured backing hook after selecting the Attach.",
			firstTarget(procedure.GetAttachId(), procedure.GetBackingServiceId()),
		)
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_DETACH:
		return projected(
			base,
			"Run backing hook: detach",
			"Run the configured backing hook while detaching the selected Attach.",
			firstTarget(procedure.GetAttachId(), procedure.GetBackingServiceId()),
		)
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_BEFORE_STOP:
		return projected(
			base,
			"Run backing hook: before stop",
			"Run the configured backing hook before stopping the selected Service.",
			firstTarget(procedure.GetServiceId(), procedure.GetAttachId(), procedure.GetBackingServiceId()),
		)
	case agentpb.BackingHookEvent_BACKING_HOOK_EVENT_AFTER_START:
		return projected(
			base,
			"Run backing hook: after start",
			"Run the configured backing hook after starting the selected Service.",
			firstTarget(procedure.GetServiceId(), procedure.GetAttachId(), procedure.GetBackingServiceId()),
		)
	default:
		return projected(
			base,
			"Run backing hook",
			"Run the configured backing lifecycle hook.",
			firstTarget(procedure.GetAttachId(), procedure.GetBackingServiceId(), procedure.GetServiceId()),
		)
	}
}

func describeBackup(base Description, authority *agentpb.BackupStepAuthority) Description {
	switch operation := authority.GetOperation().(type) {
	case *agentpb.BackupStepAuthority_Capture:
		resource := operation.Capture.GetResource()
		return projected(
			base,
			"Capture and upload backup source",
			"Capture "+backupResourcePurpose(resource.GetKind())+" and upload its backup artifact.",
			resource.GetResourceId(),
		)
	case *agentpb.BackupStepAuthority_Restore:
		resource := operation.Restore.GetDestination()
		return projected(
			base,
			"Restore backup source",
			"Restore a retained backup artifact to "+backupResourcePurpose(resource.GetKind())+".",
			resource.GetResourceId(),
		)
	case *agentpb.BackupStepAuthority_Prune:
		objects := operation.Prune.GetObjects()
		return projected(
			base,
			"Prune retained backup objects",
			pruneDescription(len(objects)),
			singlePruneTarget(objects),
		)
	default:
		return projected(
			base,
			"Run backup procedure",
			"Run the sealed backup procedure selected by the Task.",
			"",
		)
	}
}

func backupResourcePurpose(kind agentpb.BackupResourceKind) string {
	switch kind {
	case agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH:
		return "the selected Attach"
	case agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT:
		return "the selected Environment configuration"
	case agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME:
		return "the selected Volume"
	default:
		return "the selected backup source"
	}
}

func pruneDescription(count int) string {
	if count == 1 {
		return "Delete one retained backup object selected by the retention policy."
	}
	if count > 1 {
		return fmt.Sprintf("Delete %d retained backup objects selected by the retention policy.", count)
	}
	return "Apply the retention policy to the selected backup objects."
}

func singlePruneTarget(objects []*agentpb.BackupPruneObject) string {
	if len(objects) != 1 || objects[0] == nil {
		return ""
	}
	return objects[0].GetPointId()
}
