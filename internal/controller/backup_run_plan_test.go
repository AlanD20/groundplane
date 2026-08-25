package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: the locked source maximum must be enforced before schema dispatch.
func TestBuildBackupRunPlanRejectsMoreThanTwelveSources(t *testing.T) {
	sources := make([]etcd.BackupRunSourceAttemptRecord, 13)
	taskSteps := make([]etcd.TaskStepRecord, 13)
	plan, err := BuildBackupRunPlan(BackupRunPlanInput{
		Task: etcd.TaskRecord{
			Type:     etcd.TaskBackup,
			Executor: etcd.TaskExecutorAgent,
			Actor:    etcd.TaskActorOperator,
			Status:   etcd.TaskStatusPending,
			Steps:    taskSteps,
		},
		Run: etcd.BackupRunRecord{
			State:     etcd.BackupRunQueued,
			Initiator: etcd.BackupRunInitiatorOperator,
			Sources:   sources,
		},
	})
	if err == nil || plan != nil {
		t.Fatal("BuildBackupRunPlan accepted more than twelve sources")
	}
}

// Rationale: internal preparation must not publish a Task the deployed Agent cannot execute.
func TestBackupRunPlanFailsClosedWithoutAgentSchema2(t *testing.T) {
	if err := requireBackupAgentSchema2(); err == nil {
		t.Fatal("schema2 dependency unexpectedly available")
	}
}
