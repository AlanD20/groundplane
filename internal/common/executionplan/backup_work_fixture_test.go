package executionplan

import (
	"bytes"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func nativeBackupFixture(t *testing.T) (*agentpb.BackupWorkInput, workexecution.Attempt) {
	t.Helper()
	plan := validBackupPlan(t)
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	plan.Steps[0].GetBackupStep().StepDeadlineUnixNano = uint64(at.Add(time.Minute).UnixNano())
	resealBackupExecutionStep(t, plan.Steps[0])
	scope := plan.BackupScope
	input, raw, reference, err := CaptureBackupWorkInput(ids.New(ids.KindPlan), &agentpb.BackupWorkInput{
		Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE,
		ProjectId: scope.ProjectId, Project: scope.Project, EnvironmentId: scope.EnvironmentId,
		Environment: scope.Environment, Services: scope.Services,
		Steps: []*agentpb.BackupStepAuthority{plan.Steps[0].GetBackupStep()}, Artifacts: plan.Artifacts,
	})
	clear(raw)
	if err != nil {
		t.Fatal(err)
	}
	attempt := workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		Number: 1, Executor: workexecution.Identity{Executor: workexecution.Agent,
			HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1},
		Direction: workexecution.Forward, Input: reference, AssignedAt: at, StartedAt: &at,
		Deadline: at.Add(time.Minute),
	}
	return input, attempt
}

func nativeBackupCheckpoint(
	t *testing.T,
) (*agentpb.BackupWorkCheckpointRequest, *agentpb.BackupWorkInput, workexecution.Identity) {
	t.Helper()
	input, attempt := nativeBackupFixture(t)
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	content := testBackupConfigContent()
	storedSize, err := backupconfig.AgeStoredSize(content.SourceSizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	request := &agentpb.BackupWorkCheckpointRequest{
		Attempt: wire, CheckpointSequence: 1,
		Checkpoint: &agentpb.BackupWorkCheckpointRequest_ArtifactPrepared{
			ArtifactPrepared: &agentpb.BackupArtifactPrepared{
				PointId: testBackupPointID,
				Evidence: &agentpb.BackupArtifactEvidence{
					SourceSizeBytes: content.SourceSizeBytes, SourceSha256: bytes.Repeat([]byte{21}, sha256.Size),
					StoredSizeBytes: storedSize, StoredSha256: bytes.Repeat([]byte{22}, sha256.Size),
				},
				Finals: &agentpb.BackupStagingFinals{
					SourceRelativeName: BackupSourceStagingFinal, StoredRelativeName: BackupStoredStagingFinal,
					SameInode: backupBool(false),
				},
				Archive: &agentpb.BackupArtifactPrepared_Config{Config: &agentpb.BackupConfigArchiveEvidence{
					Content: content, CaptureTranscriptSha256: bytes.Repeat([]byte{23}, sha256.Size),
				}},
			},
		},
	}
	return request, input, attempt.Executor
}

func nativeBackupUploadCheckpoint(
	t *testing.T,
) (*agentpb.BackupWorkCheckpointRequest, *agentpb.BackupWorkInput, workexecution.Identity) {
	t.Helper()
	request, input, identity := nativeBackupCheckpoint(t)
	target := proto.CloneOf(input.Steps[0].GetCapture().Target)
	evidence := proto.CloneOf(request.GetArtifactPrepared().Evidence)
	request.Checkpoint = &agentpb.BackupWorkCheckpointRequest_UploadCompleted{
		UploadCompleted: &agentpb.BackupUploadCompleted{
			PointId: testBackupPointID, Target: target, Evidence: evidence,
			Outcome: &agentpb.BackupUploadCompleted_ReturnedObject{ReturnedObject: &agentpb.BackupObjectIdentity{
				Connector: proto.CloneOf(target.Connector), Bucket: target.Bucket, ObjectKey: target.ObjectKey,
				Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}},
			}},
			MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{25}, sha256.Size),
		},
	}
	return request, input, identity
}

func nativeBackupCheckpointAck(
	t *testing.T,
	request *agentpb.BackupWorkCheckpointRequest,
	input *agentpb.BackupWorkInput,
) *agentpb.BackupWorkCheckpointAck {
	t.Helper()
	digest, err := BackupWorkCheckpointDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	step := input.Steps[request.SourceOrdinal]
	fence := &agentpb.CheckpointFence{AuthorityDigest: bytes.Clone(step.StepDigest), DedupeKeyModRevision: 12}
	prepared := proto.CloneOf(request.GetArtifactPrepared())
	return &agentpb.BackupWorkCheckpointAck{
		Attempt: proto.CloneOf(request.Attempt), SourceOrdinal: request.SourceOrdinal,
		CheckpointSequence: request.CheckpointSequence, Committed: fence, RequestSha256: digest,
		Source: &agentpb.BackupStepResume{
			StepId: step.StepId, ExecutionId: step.ExecutionId,
			Operation: &agentpb.BackupStepResume_Capture{Capture: &agentpb.BackupCaptureResume{
				CheckpointSequence: request.CheckpointSequence, PrecedingCheckpoint: proto.CloneOf(fence),
				Phase:  agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_ARTIFACT_PREPARED,
				Cursor: &agentpb.BackupCaptureCursor{ObjectAttempt: 1}, PreparedArtifact: prepared,
				Checkpoint: &agentpb.BackupCaptureResume_ArtifactPrepared{ArtifactPrepared: proto.CloneOf(prepared)},
			}},
		},
	}
}
