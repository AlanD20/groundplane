package taskplanning

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// DescribeTaskSteps projects only operation names and timeouts from the exact
// retained plan. Commands, file content and credential-bearing inputs stay private.
func (resolver *TaskPlanResolver) DescribeTaskSteps(
	ctx context.Context,
	task etcd.TaskRecord,
) (map[string]apiTypes.TaskStep, error) {
	plan, err := resolver.ResolveExecutionPlan(ctx, task)
	if err != nil {
		return nil, err
	}
	if plan == nil || hex.EncodeToString(plan.PlanHash) != task.PlanHash {
		return nil, errs.New(errs.KindStateConflict, "Task execution descriptions require the exact sealed plan")
	}
	descriptions := make(map[string]apiTypes.TaskStep, len(plan.Steps))
	for _, step := range plan.Steps {
		descriptions[step.StepId] = apiTypes.TaskStep{
			Action:         executionStepAction(step),
			TimeoutSeconds: step.TimeoutSeconds,
		}
	}
	return descriptions, nil
}

func executionStepAction(step *agentpb.ExecutionStep) string {
	switch payload := step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply:
		return "Apply container configuration"
	case *agentpb.ExecutionStep_ComposeStop:
		return "Stop workload containers"
	case *agentpb.ExecutionStep_ComposeRemove:
		return "Remove workload containers"
	case *agentpb.ExecutionStep_WaitHealthy, *agentpb.ExecutionStep_WaitWorkloadHealthy:
		return "Wait for workload health"
	case *agentpb.ExecutionStep_EnvironmentDirectoryCreate:
		return "Create Environment directory"
	case *agentpb.ExecutionStep_MaterializeFile:
		return "Update managed configuration file"
	case *agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure:
		return "Prepare Volume directories"
	case *agentpb.ExecutionStep_EnvironmentDirectoryRemove:
		return "Remove Environment directory"
	case *agentpb.ExecutionStep_AdapterProcedure:
		return "Run " + payload.AdapterProcedure.AdapterKey + " adapter: " + strings.ToLower(strings.TrimPrefix(payload.AdapterProcedure.Phase.String(), "ADAPTER_PROCEDURE_PHASE_"))
	case *agentpb.ExecutionStep_ManagedNetworkRemove:
		return "Remove Zone network"
	case *agentpb.ExecutionStep_ComposeWorkloadApply:
		return "Start workload containers"
	case *agentpb.ExecutionStep_ServiceProxySwitch:
		return "Switch Service proxy to deployed workload"
	case *agentpb.ExecutionStep_ServiceProxyProbe:
		return "Verify Service proxy traffic"
	case *agentpb.ExecutionStep_ServiceProxyCompensate:
		return "Restore previous Service proxy"
	case *agentpb.ExecutionStep_ServiceRecreateAcknowledge:
		return "Record deployed Service runtime"
	case *agentpb.ExecutionStep_ServiceRecreateProbe:
		return "Verify recreated Service runtime"
	case *agentpb.ExecutionStep_ServiceRecreateCompensate:
		return "Restore previous Service runtime"
	case *agentpb.ExecutionStep_ManagedVolumeRemove:
		return "Remove managed Volume"
	case *agentpb.ExecutionStep_ManagedVolumeDirectoryRemove:
		return "Remove Volume directory"
	case *agentpb.ExecutionStep_ComponentApply:
		return "Apply Component configuration"
	case *agentpb.ExecutionStep_HostResolutionApply:
		return "Apply host DNS configuration"
	case *agentpb.ExecutionStep_HostResolutionRestore:
		return "Restore host DNS configuration"
	case *agentpb.ExecutionStep_RunScript:
		return "Run configured script"
	case *agentpb.ExecutionStep_CandidateRestorationProbe:
		return "Verify previous workload restoration"
	case *agentpb.ExecutionStep_CandidateRestorationCompensate:
		return "Restore previous workload"
	case *agentpb.ExecutionStep_ManagedNetworkEnsure:
		return "Prepare Zone network"
	case *agentpb.ExecutionStep_ManagedVolumeEnsure:
		return "Prepare managed Volume"
	case *agentpb.ExecutionStep_BackingHookProcedure:
		return "Run backing hook: " + strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(payload.BackingHookProcedure.Event.String(), "BACKING_HOOK_EVENT_")), "_", " ")
	case *agentpb.ExecutionStep_BackupStep:
		switch payload.BackupStep.Operation.(type) {
		case *agentpb.BackupStepAuthority_Capture:
			return "Capture and upload backup source"
		case *agentpb.BackupStepAuthority_Restore:
			return "Restore backup source"
		case *agentpb.BackupStepAuthority_Prune:
			return "Prune retained backup objects"
		}
	}
	return "Execution details unavailable"
}
