package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func operationMatchesTask(operation agentpb.PlanOperation, task etcd.TaskRecord) bool {
	switch task.Type {
	case taskjournal.TaskScript:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_SCRIPT
	case taskjournal.TaskDeploy:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DEPLOY
	case taskjournal.TaskRollback:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK
	case taskjournal.TaskStart:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_START
	case taskjournal.TaskStop:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_STOP
	case taskjournal.TaskDestroy:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DESTROY
	case taskjournal.TaskRemove:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_REMOVE
	case taskjournal.TaskCreate:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE ||
			task.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceVolume &&
				operation == agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	case taskjournal.TaskAttach:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_ATTACH
	case taskjournal.TaskDetach:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_DETACH
	case taskjournal.TaskUpdate:
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
		_, backingCreation := task.Params[taskjournal.TaskBackingServiceCreationParam]
		return backingCreation && operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	case taskjournal.TaskBackup:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP
	case taskjournal.TaskBackupPrune:
		return operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE
	default:
		return false
	}
}
