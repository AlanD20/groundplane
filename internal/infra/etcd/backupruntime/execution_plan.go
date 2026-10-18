package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupExecutionPlanKey holds the immutable procedure published atomically
// with the Task. Reconnect reads this procedure rather than rebuilding it from
// progress records or current desired state.
func BackupExecutionPlanKey(taskID string) string {
	return "/v1/runtime/backup-execution-plans/" + taskID
}

func EncodeBackupExecutionPlan(plan *agentpb.ExecutionPlan) ([]byte, error) {
	validated, err := executionplan.Validate(plan)
	if err != nil {
		return nil, err
	}
	switch validated.Operation {
	case agentpb.PlanOperation_PLAN_OPERATION_BACKUP,
		agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE,
		agentpb.PlanOperation_PLAN_OPERATION_RESTORE:
	default:
		return nil, errs.New(errs.KindValidationFailed, "backup procedure operation is invalid")
	}
	return (proto.MarshalOptions{Deterministic: true}).Marshal(validated)
}

func DecodeBackupExecutionPlan(value []byte) (*agentpb.ExecutionPlan, error) {
	if len(value) == 0 || len(value) > executionplan.MaximumPlanBytes {
		return nil, CorruptBackupRuntimeRecord()
	}
	plan := &agentpb.ExecutionPlan{}
	if err := proto.Unmarshal(value, plan); err != nil {
		return nil, CorruptBackupRuntimeRecord()
	}
	if _, err := EncodeBackupExecutionPlan(plan); err != nil {
		return nil, CorruptBackupRuntimeRecord()
	}
	return plan, nil
}
