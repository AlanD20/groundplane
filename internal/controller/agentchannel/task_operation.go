package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func operationMatchesTask(operation agentpb.PlanOperation, task etcd.TaskRecord) bool {
	switch task.Type {
	case etcd.TaskScript:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_SCRIPT
	case etcd.TaskDeploy:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DEPLOY
	case etcd.TaskRollback:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	case etcd.TaskStart:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_START
	case etcd.TaskStop:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_STOP
	case etcd.TaskDestroy:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DESTROY
	case etcd.TaskRemove:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	case etcd.TaskCreate:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE ||
			task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceVolume &&
				operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	case etcd.TaskAttach:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ATTACH
	case etcd.TaskDetach:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DETACH
	case etcd.TaskUpdate:
		// Direct resource mutations have their own sealed plan procedures. They
		// are not unmarked Environment reconciliation or Blueprint Apply.
		switch task.Params[etcd.TaskResourceKindParam] {
		case etcd.TaskResourceVolume:
			return ids.Validate(ids.KindVolume, task.Target) == nil &&
				operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
		case etcd.TaskResourceEntry:
			return ids.Validate(ids.KindEnvironment, task.Target) == nil &&
				operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
		}
		procedure, validProcedure := taskcontract.ParseBlueprintComposeProcedure(
			task.Params[taskcontract.EnvironmentBlueprintProcedureParam],
		)
		if operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
			return ids.Validate(ids.KindEnvironment, task.Target) == nil && validProcedure &&
				(procedure == taskcontract.BlueprintComposeProcedureNone ||
					procedure == taskcontract.BlueprintComposeProcedureCandidateReleases)
		}
		if operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE {
			return ids.Validate(ids.KindEnvironment, task.Target) == nil && validProcedure &&
				procedure == taskcontract.BlueprintComposeProcedureFullReconcile
		}
		if task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceComponent {
			return operation == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY
		}
		_, backingCreation := task.Params[etcd.TaskBackingServiceCreationParam]
		return backingCreation && operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	case etcd.TaskBackup:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP
	case etcd.TaskBackupPrune:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE
	default:
		return false
	}
}
