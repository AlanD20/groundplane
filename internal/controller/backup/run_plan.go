package backup

import (
	"bytes"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"

	"google.golang.org/protobuf/proto"
)

const (
	backupRunTaskTimeoutSeconds int64 = 6 * 60 * 60
)

// BackupRunPlanInput contains the already-persisted task and fixed plan
// evidence. Source order and all source identities come from the run record;
// the caller cannot add, remove, or reorder captures here. Scope and Authority
// come from that same versioned publication snapshot; retries reuse the sealed
// plan rather than selecting newer inputs.
type BackupRunPlanInput struct {
	Task      etcd.TaskRecord
	Run       backupruntime.BackupRunRecord
	Scope     *agentpb.BackupPlanScope
	Authority []*agentpb.BackupStepAuthority
	Artifacts []*agentpb.ComposeArtifact
}

// BuildBackupRunPlan builds and seals the immutable agent plan for a backup
// task. Credential and secret plaintext are never part of the plan.
func BuildBackupRunPlan(input BackupRunPlanInput) (*agentpb.ExecutionPlan, error) {
	task := input.Task
	run := input.Run
	if task.Type != taskjournal.TaskBackup {
		return nil, errs.New(errs.KindValidationFailed, "backup run task type must be backup")
	}
	if task.Executor != taskjournal.TaskExecutorAgent {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task must be agent-executed",
		)
	}
	switch {
	case run.RetryOfTaskID != "":
		if task.Actor != taskjournal.TaskActorOperator || task.RetryOf != run.RetryOfTaskID {
			return nil, errs.New(
				errs.KindValidationFailed,
				"backup retry task must be operator-acted and name its source",
			)
		}
	case run.Initiator == backupruntime.BackupRunInitiatorOperator:
		if task.Actor != taskjournal.TaskActorOperator {
			return nil, errs.New(errs.KindValidationFailed, "operator backup run task must be operator-acted")
		}
	case run.Initiator == backupruntime.BackupRunInitiatorSchedule:
		if task.Actor != taskjournal.TaskActorSystem {
			return nil, errs.New(errs.KindValidationFailed, "scheduled backup run task must be system-acted")
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "backup run initiator is invalid")
	}
	if task.Status != taskjournal.TaskStatusPending && task.Status != taskjournal.TaskStatusRunning {
		return nil, errs.New(errs.KindValidationFailed, "backup run task status must be pending or running")
	}
	if task.Target == "" || task.Target != run.EnvironmentID {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task target must match run environment",
		)
	}
	if task.ID == "" || task.OperationID == "" || task.PlanID == "" || run.TaskID != task.ID ||
		run.OperationID != task.OperationID || task.RetryOf != run.RetryOfTaskID {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task and run identities do not match",
		)
	}
	if task.RenderGeneration != 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task plan metadata is not unrendered",
		)
	}
	if task.TimeoutSeconds != backupRunTaskTimeoutSeconds {
		return nil, errs.New(errs.KindValidationFailed, "backup run task timeout must be six hours")
	}
	if len(task.Params) != 0 || len(task.Materializations) != 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task params and materializations must be empty",
		)
	}
	if !((task.Status == taskjournal.TaskStatusPending && run.State == backupruntime.BackupRunQueued) ||
		(task.Status == taskjournal.TaskStatusRunning && run.State == backupruntime.BackupRunRunning)) ||
		(run.Initiator != backupruntime.BackupRunInitiatorOperator && run.Initiator != backupruntime.BackupRunInitiatorSchedule) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run state must match its pending or running task and have a valid initiator",
		)
	}
	if len(run.Sources) == 0 || len(run.Sources) > executionplan.MaximumBackupSources {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run source count must be between one and twelve",
		)
	}
	if len(task.Steps) != len(run.Sources) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run task step count must match source count",
		)
	}
	if input.Scope == nil || input.Scope.GetEnvironmentId() != run.EnvironmentID ||
		len(input.Authority) != len(run.Sources) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup run requires its frozen scope and ordered step authority",
		)
	}
	if ids.Validate(ids.KindTask, task.ID) != nil ||
		ids.Validate(ids.KindOperation, task.OperationID) != nil ||
		ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backup run task identities are invalid")
	}

	plan := &agentpb.ExecutionPlan{
		Schema:      executionplan.SchemaVersion,
		PlanId:      task.PlanID,
		Operation:   agentpb.PlanOperation_PLAN_OPERATION_BACKUP,
		TargetId:    run.EnvironmentID,
		BackupScope: proto.CloneOf(input.Scope),
		Artifacts:   proto.CloneOf(&agentpb.ExecutionPlan{Artifacts: input.Artifacts}).Artifacts,
		Steps:       make([]*agentpb.ExecutionStep, 0, len(run.Sources)),
	}
	for index, source := range run.Sources {
		step := task.Steps[index]
		if ids.Validate(ids.KindStep, step.ID) != nil || source.Ordinal != uint32(index) {
			return nil, errs.New(errs.KindValidationFailed, "backup run step identity is invalid")
		}
		authority := input.Authority[index]
		if authority == nil || authority.GetStepId() != step.ID || task.CreatedAt.IsZero() ||
			authority.GetStepDeadlineUnixNano() != uint64(
				task.CreatedAt.Add(time.Duration(backupRunTaskTimeoutSeconds)*time.Second).UnixNano(),
			) {
			return nil, errs.New(errs.KindValidationFailed, "backup run step identity or absolute deadline is invalid")
		}
		var err error
		if len(authority.GetStepDigest()) == 0 {
			authority, err = executionplan.SealBackupStepAuthority(authority)
		} else {
			authority, err = executionplan.ValidateBackupStepAuthority(authority)
		}
		if err != nil {
			return nil, err
		}
		if err := backupplanning.ValidateBackupRunStepAuthority(run, source, authority, input.Scope); err != nil {
			return nil, err
		}
		plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{
			StepId:         step.ID,
			TimeoutSeconds: uint32(backupRunTaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_BackupStep{
				BackupStep: authority,
			},
		})
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		return nil, err
	}
	if task.PlanHash != "" {
		durableHash, decodeErr := hex.DecodeString(task.PlanHash)
		if decodeErr != nil || !bytes.Equal(durableHash, sealed.PlanHash) {
			return nil, errs.New(
				errs.KindStateConflict, "backup run sealed plan changed during reconnect",
			)
		}
	}
	return sealed, nil
}
