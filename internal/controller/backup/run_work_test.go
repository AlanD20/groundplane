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
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"

	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func backupRunWorkFixture(t *testing.T, request BackupRunPrepareInput) BackupRunPrepared {
	t.Helper()
	now := request.CreatedAt
	if now.IsZero() {
		now = time.Date(2026, 8, 27, 12, 0, 0, 0, time.UTC)
	}
	environmentID, taskID, operationID := request.EnvironmentID, request.TaskID, request.OperationID
	if environmentID == "" {
		environmentID = ids.NewAt(ids.KindEnvironment, now, 1)
	}
	if taskID == "" {
		taskID = ids.NewAt(ids.KindTask, now, 2)
	}
	if operationID == "" {
		operationID = ids.NewAt(ids.KindOperation, now, 3)
	}
	stepID := ids.NewAt(ids.KindStep, now, 5)
	sourceID := ids.NewAt(ids.KindBackupSource, now, 6)
	pointID := ids.NewAt(ids.KindRecoveryPoint, now, 7)
	connectorID := ids.NewAt(ids.KindConnector, now, 8)
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
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
	prepared := BackupRunPrepared{
		HostID: ids.New(ids.KindHost), Run: run,
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
	prepared.Owner = testtaskjournal.TaskOwner{ProjectID: prepared.Scope.ProjectId, EnvironmentID: environmentID}
	prepared.Authority[0], err = executionplan.SealBackupStepAuthority(prepared.Authority[0])
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

// TASK-101: mutable source progress cannot alter the native immutable input.
// This is capture stability, not restart/transport or source-effect proof.
func TestBackupWorkCaptureIgnoresMutableSourceProgress(t *testing.T) {
	input := backupRunWorkFixture(t, BackupRunPrepareInput{})
	inputID := ids.New(ids.KindPlan)
	pending, _, captured, err := buildBackupRunWork(input, inputID)
	if err != nil {
		t.Fatal(err)
	}

	input.Run.State = testbackupruntime.BackupRunRunning
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
	running, _, recaptured, err := buildBackupRunWork(input, inputID)
	if err != nil {
		t.Fatal(err)
	}
	if running.Intent != pending.Intent || !proto.Equal(captured, recaptured) {
		t.Fatal("source progress changed immutable Backup input")
	}
}

// TASK-101: prove the valid maximum before refusing one additional source.
func TestBackupWorkCaptureEnforcesTwelveSourceBoundary(t *testing.T) {
	input := backupRunWorkFixture(t, BackupRunPrepareInput{})
	first := input.Authority[0]
	for index := 1; index < 13; index++ {
		step := proto.CloneOf(first)
		step.StepDigest = nil
		step.StepId, step.ExecutionId = ids.New(ids.KindStep), ids.NewULID()
		step.GetCapture().PointId = ids.New(ids.KindRecoveryPoint)
		step.GetCapture().Target.ObjectKey = "objects/" + input.Run.EnvironmentID + "/" +
			ids.New(ids.KindBackupSource) + "/" + step.GetCapture().PointId + "/artifact.bin"
		var err error
		step, err = executionplan.SealBackupStepAuthority(step)
		if err != nil {
			t.Fatal(err)
		}
		input.Authority = append(input.Authority, step)
	}
	inputID := ids.New(ids.KindPlan)
	maximum := input
	maximum.Authority = input.Authority[:12]
	if _, _, _, err := buildBackupRunWork(maximum, inputID); err != nil {
		t.Fatalf("valid twelve-source capture rejected: %v", err)
	}
	if _, _, captured, err := buildBackupRunWork(input, inputID); err == nil || captured != nil {
		t.Fatal("thirteen sources produced executable Backup input")
	}
}
