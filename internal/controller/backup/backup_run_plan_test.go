package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the locked source maximum must be enforced before schema dispatch.
func TestBuildBackupRunPlanRejectsMoreThanTwelveSources(t *testing.T) {
	input := backupRunPlanFixture(t)
	if _, err := BuildBackupRunPlan(input); err != nil {
		t.Fatalf("valid starting plan: %v", err)
	}
	input.Run.Sources = make([]testbackupruntime.BackupRunSourceAttemptRecord, 13)
	input.Task.Steps = make([]testtaskjournal.TaskStepRecord, 13)
	plan, err := BuildBackupRunPlan(input)
	if err == nil || plan != nil {
		t.Fatal("BuildBackupRunPlan accepted more than twelve sources")
	}
}

func backupRunPlanFixture(t *testing.T) BackupRunPlanInput {
	t.Helper()
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
		Steps:     []testtaskjournal.TaskStepRecord{{Kind: testtaskjournal.TaskStepOperation, ID: stepID}},
		CreatedAt: now,
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
		BackupKeyRecordRevision: 23, BackupKeyValueRevision: 24, CreatedAt: now,
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
	revision := func(value int64) *agentpb.RevisionDigest {
		return &agentpb.RevisionDigest{ModRevision: value, Sha256: bytes.Repeat([]byte{0x2a}, 32)}
	}
	pathStyle := run.ConnectorPathStyle
	era := uint64(run.KeyEra)
	recipientDigest := sha256.Sum256([]byte(run.Recipient))
	return BackupRunPlanInput{
		Task: task, Run: run,
		Scope: &agentpb.BackupPlanScope{
			ProjectId: ids.NewAt(ids.KindProject, now, 10), Project: revision(18),
			EnvironmentId: environmentID, Environment: revision(22), TaskAttempt: 1,
		},
		Authority: []*agentpb.BackupStepAuthority{{
			StepId: stepID, ExecutionId: ids.NewULID(),
			StepDeadlineUnixNano: uint64(now.Add(6 * time.Hour).UnixNano()),
			Operation: &agentpb.BackupStepAuthority_Capture{Capture: &agentpb.BackupCaptureAuthority{
				PointId: pointID,
				Resource: &agentpb.BackupResourceIdentity{
					Kind:       agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT,
					ResourceId: environmentID, Resource: revision(22),
				},
				Target: &agentpb.BackupObjectTarget{
					Bucket: run.ConnectorBucket, ObjectKey: run.Sources[0].ObjectKey,
					Connector: &agentpb.BackupConnectorAuthority{
						ConnectorId: connectorID, Connector: revision(run.ConnectorRevision),
						CanonicalEndpointUrl: run.ConnectorEndpoint, Region: run.ConnectorRegion,
						Prefix: run.ConnectorPrefix, PathStyle: &pathStyle,
						AccessKeySlotId: "access", SecretKeySlotId: "secret",
						AccessKeySlot: revision(20), SecretKeySlot: revision(20),
					},
				},
				Encryption: &agentpb.BackupEncryptionAuthority{
					Kind:         agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
					SecretSlotId: backupsecret.CurrentAgeIdentitySlotID,
					SecretSlot: revision(
						run.BackupKeyValueRevision,
					), RecipientSha256: recipientDigest[:], KeyEra: &era,
				},
				Source: &agentpb.BackupCaptureAuthority_Config{Config: &agentpb.BackupConfigCaptureAuthority{
					EnvironmentId: environmentID, MetadataSnapshotRevision: 22,
					Content: &agentpb.BackupConfigContentAuthority{
						ManifestSha256: bytes.Repeat(
							[]byte{0x2a},
							32,
						), MetadataSnapshotSha256: bytes.Repeat([]byte{0x2b}, 32),
						ManifestSizeBytes: 128, SourceSizeBytes: 4096,
					},
				}},
			}},
		}},
	}
}

// Rationale: reconnect after durable source progress must rebuild the identical
// sealed plan while accepting the running Task and its already stored hash.
func TestBuildBackupRunPlanReconnectAfterProgressIsIdentical(t *testing.T) {
	input := backupRunPlanFixture(t)
	pending, err := BuildBackupRunPlan(input)
	if err != nil {
		t.Fatal(err)
	}

	input.Task.Status = testtaskjournal.TaskStatusRunning
	input.Run.State = testbackupruntime.BackupRunRunning
	input.Task.PlanHash = hex.EncodeToString(pending.PlanHash)
	input.Run.Sources[0].State = testbackupruntime.BackupSourceAttemptStaged
	input.Run.Sources[0].Phase = testbackupruntime.BackupSourcePhaseUpload
	storedSize, err := backupformat.AgeStoredSize(4096)
	if err != nil {
		t.Fatal(err)
	}
	input.Run.Sources[0].Evidence = testbackupruntime.BackupArtifactEvidence{
		SourceSizeBytes: 4096, SourceSHA256: hex.EncodeToString(bytes.Repeat([]byte{0x2a}, 32)),
		StoredSizeBytes: storedSize, StoredSHA256: hex.EncodeToString(bytes.Repeat([]byte{0x2b}, 32)),
	}
	running, err := BuildBackupRunPlan(input)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(running.PlanHash, pending.PlanHash) || !proto.Equal(running, pending) {
		t.Fatal("running reconnect changed the sealed Backup plan")
	}
}
