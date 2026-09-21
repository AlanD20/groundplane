package backup

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"google.golang.org/protobuf/proto"
)

// Rationale: the locked source maximum must be enforced before schema dispatch.
func TestBuildBackupRunPlanRejectsMoreThanTwelveSources(t *testing.T) {
	sources := make([]testbackupruntime.BackupRunSourceAttemptRecord, 13)
	taskSteps := make([]testtaskjournal.TaskStepRecord, 13)
	plan, err := BuildBackupRunPlan(BackupRunPlanInput{
		Task: etcd.TaskRecord{
			Type:     testtaskjournal.TaskBackup,
			Executor: testtaskjournal.TaskExecutorAgent,
			Actor:    testtaskjournal.TaskActorOperator,
			Status:   testtaskjournal.TaskStatusPending,
			Steps:    taskSteps,
		},
		Run: testbackupruntime.BackupRunRecord{
			State:     testbackupruntime.BackupRunQueued,
			Initiator: testbackupruntime.BackupRunInitiatorOperator,
			Sources:   sources,
		},
	})
	if err == nil || plan != nil {
		t.Fatal("BuildBackupRunPlan accepted more than twelve sources")
	}
}

// Rationale: reconnect after durable source progress must rebuild the identical
// sealed plan while accepting the running Task and its already stored hash.
func TestBuildBackupRunPlanReconnectAfterProgressIsIdentical(t *testing.T) {
	now := time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	taskID := ids.NewAt(ids.KindTask, now, 2)
	operationID := ids.NewAt(ids.KindOperation, now, 3)
	planID := ids.NewAt(ids.KindPlan, now, 4)
	stepID := ids.NewAt(ids.KindStep, now, 5)
	sourceID := ids.NewAt(ids.KindBackupSource, now, 6)
	pointID := ids.NewAt(ids.KindRecoveryPoint, now, 7)
	connectorID := ids.NewAt(ids.KindConnector, now, 8)
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, PlanID: planID,
		Type: testtaskjournal.TaskBackup, Target: environmentID,
		Executor: testtaskjournal.TaskExecutorAgent, Actor: testtaskjournal.TaskActorOperator,
		Status: testtaskjournal.TaskStatusPending, TimeoutSeconds: backupRunTaskTimeoutSeconds,
		Steps: []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}},
	}
	run := testbackupruntime.BackupRunRecord{
		TaskID: taskID, OperationID: operationID, EnvironmentID: environmentID,
		PolicyRevision: 19, RetentionKeep: 7,
		Initiator:   testbackupruntime.BackupRunInitiatorOperator,
		ConnectorID: connectorID, ConnectorRevision: 20,
		ConnectorEndpoint: "https://s3.example.test",
		ConnectorBucket:   "groundplane-backups", ConnectorPrefix: "objects/",
		ConnectorRegion: "auto",
		Encryption:      testbackupruntime.BackupRuntimeEncryptionAge,
		KeyEra:          1, Recipient: identity.Recipient().String(),
		State: testbackupruntime.BackupRunQueued,
		Sources: []testbackupruntime.BackupRunSourceAttemptRecord{{
			Ordinal: 0, SourceID: sourceID,
			Kind: testbackupruntime.BackupRuntimeSourceConfig, TargetID: environmentID,
			SourceRevision: 21, TargetRevision: 22,
			Snapshot: testbackupruntime.BackupRunSourceSnapshot{
				Config: &testbackupruntime.BackupConfigSourceSnapshot{
					ConfigSnapshotID: ids.NewAt(ids.KindConfig, now, 9),
					ReadRevision:     22,
				},
			},
			Format:          testbackupruntime.BackupRuntimeFormatConfig,
			RecoveryPointID: pointID,
			ObjectKey:       "objects/" + environmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
			State:           testbackupruntime.BackupSourceAttemptPending,
			Phase:           testbackupruntime.BackupSourcePhaseCapture,
		}},
	}
	input := BackupRunPlanInput{
		Task: task, Run: run, Upload: BackupRunUploadAuthorities(run),
	}
	pending, err := BuildBackupRunPlan(input)
	if err != nil {
		t.Fatal(err)
	}

	input.Task.Status = testtaskjournal.TaskStatusRunning
	input.Run.State = testbackupruntime.BackupRunRunning
	input.Task.PlanHash = hex.EncodeToString(pending.PlanHash)
	input.Run.Sources[0].State = testbackupruntime.BackupSourceAttemptStaged
	input.Run.Sources[0].Phase = testbackupruntime.BackupSourcePhaseUpload
	input.Run.Sources[0].SizeBytes = 4096
	input.Run.Sources[0].SHA256 = hex.EncodeToString(bytes.Repeat([]byte{0x2a}, 32))
	running, err := BuildBackupRunPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(running.PlanHash, pending.PlanHash) || !proto.Equal(running, pending) {
		t.Fatal("running reconnect changed the sealed Backup plan")
	}
}
