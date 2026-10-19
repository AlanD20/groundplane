package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// TASK-102: the native transition must pin database/tool versions before dump
// execution and reject relabelling when the resulting artifact is prepared.
// Receipts and container/version observations are supplied, not real DB effects.
func TestNativeDatabaseCapturePinsArchiveBeforePreparation(t *testing.T) {
	for _, family := range []string{"postgres", "mysql"} {
		t.Run(family, func(t *testing.T) {
			input, attempt := nativeBackupFixture(t)
			step := input.Steps[0]
			capture := step.GetCapture()
			serviceID := ids.New(ids.KindService)
			fact := validBackupServiceFactFixture(serviceID)
			labels := []*agentpb.LabelPair{{Key: "com.groundplane.managed", Value: "true"}}
			var err error
			fact.RequiredLabelsSha256, err = backupservicefact.LabelsDigest(labels)
			if err != nil {
				t.Fatal(err)
			}
			input.Services = []*agentpb.BackupServiceFact{fact}
			capture.Resource.Kind = agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ATTACH
			capture.Resource.ResourceId = ids.New(ids.KindAttach)
			imageDigest := bytes.Repeat([]byte{0xaa}, 32)
			databaseImage := "docker.io/library/mysql:8.4@sha256:" + strings.Repeat("a", 64)
			if family == "postgres" {
				databaseImage = "docker.io/library/postgres:16-alpine@sha256:" + strings.Repeat("a", 64)
			} else {
				digest := sha256.Sum256([]byte(databaseImage))
				imageDigest = digest[:]
			}
			yaml := []byte("services:\n  application:\n    image: " + databaseImage + "\n")
			yamlDigest := sha256.Sum256(yaml)
			service := &agentpb.ComposeService{ServiceId: serviceID, ComposeName: fact.CurrentName,
				Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
				ImageReference: databaseImage, ExpectedLabels: labels, ExpectedReplicas: 1}
			if family == "postgres" {
				service.PostgresToolsImage = "registry.example.test/postgres-tools@sha256:" + strings.Repeat("a", 64)
			}
			input.Artifacts = []*agentpb.ComposeArtifact{{ArtifactId: ids.New(ids.KindConfig),
				OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: input.EnvironmentId,
				CanonicalYaml: yaml, YamlSha256: yamlDigest[:], Services: []*agentpb.ComposeService{service}}}
			observed := &agentpb.BackupWorkCheckpointRequest{}
			dump := &agentpb.BackupWorkCheckpointRequest{}
			prepared := &agentpb.BackupArtifactPrepared{PointId: capture.PointId,
				Finals: &agentpb.BackupStagingFinals{SourceRelativeName: BackupSourceStagingFinal,
					StoredRelativeName: BackupStoredStagingFinal, SameInode: proto.Bool(false)},
				Evidence: &agentpb.BackupArtifactEvidence{SourceSizeBytes: 4096,
					SourceSha256: bytes.Repeat([]byte{4}, 32), StoredSha256: bytes.Repeat([]byte{5}, 32)}}
			prepared.Evidence.StoredSizeBytes, err = backupformat.AgeStoredSize(4096)
			if err != nil {
				t.Fatal(err)
			}
			if family == "postgres" {
				capture.Source = &agentpb.BackupCaptureAuthority_Postgres{
					Postgres: &agentpb.BackupPostgresCaptureAuthority{
						DatabaseServiceId: serviceID, AdapterContractVersion: 1, ManagedReleaseIndex: archiveTestRelease(t),
						DatabaseName: "app_000001", RoleName: "app_000001", MaxPlaintextBytes: backupformat.MaxAgeSourceBytes},
				}
				observed.Checkpoint = &agentpb.BackupWorkCheckpointRequest_PostgresContainerObserved{
					PostgresContainerObserved: &agentpb.BackupPostgresContainerObserved{
						ServiceId: serviceID, ContainerId: strings.Repeat("a", 64), RepositoryDigest: imageDigest,
						ObservedLabelCount: fact.RequiredLabelCount, ObservedLabelsSha256: fact.RequiredLabelsSha256,
						ObservationSha256: imageDigest, DatabaseImageIdSha256: fact.LocalImageIdSha256}}
				archive := &agentpb.BackupPostgresArchiveEvidence{PgDumpMajor: 16, AdapterContractVersion: 1,
					SourceServerVersion: "16.9", BackupToolVersion: "16.9"}
				dump.Checkpoint = &agentpb.BackupWorkCheckpointRequest_PostgresDumpStart{
					PostgresDumpStart: &agentpb.BackupPostgresDumpStart{
						PointId: capture.PointId, ContainerId: strings.Repeat("a", 64), ExecId: strings.Repeat("b", 64),
						RepositoryDigest: imageDigest, ExpectedLabelsSha256: fact.RequiredLabelsSha256,
						ExecutionNonce: imageDigest, AdapterContractVersion: 1, DatabaseName: "app_000001",
						RoleName: "app_000001", MaxPlaintextBytes: backupformat.MaxAgeSourceBytes, Archive: archive}}
				prepared.Archive = &agentpb.BackupArtifactPrepared_Postgres{Postgres: proto.CloneOf(archive)}
			} else {
				capture.Source = &agentpb.BackupCaptureAuthority_Mysql{Mysql: &agentpb.BackupMySQLCaptureAuthority{
					DatabaseServiceId: serviceID, AdapterContractVersion: 1, RequiredServerMajor: 8, RequiredServerMinor: 4,
					DatabaseName: "app_000001", RoleName: "app_000001", MaxPlaintextBytes: backupformat.MaxAgeSourceBytes,
					DatabaseImageReferenceSha256: imageDigest}}
				observed.Checkpoint = &agentpb.BackupWorkCheckpointRequest_MysqlContainerObserved{
					MysqlContainerObserved: &agentpb.BackupMySQLContainerObserved{
						ServiceId: serviceID, ContainerId: strings.Repeat("a", 64), ImageReferenceSha256: imageDigest,
						ObservedLabelCount: fact.RequiredLabelCount, ObservedLabelsSha256: fact.RequiredLabelsSha256,
						ObservationSha256: imageDigest, DatabaseImageIdSha256: fact.LocalImageIdSha256}}
				archive := &agentpb.BackupMySQLArchiveEvidence{AdapterContractVersion: 1, SourceServerVersion: "8.4.5",
					BackupToolVersion: "mysqldump  Ver 8.4.5 for Linux on x86_64", ArtifactFormat: "mysql-logical-v1"}
				dump.Checkpoint = &agentpb.BackupWorkCheckpointRequest_MysqlDumpStart{MysqlDumpStart: &agentpb.BackupMySQLDumpStart{
					PointId: capture.PointId, ContainerId: strings.Repeat("a", 64), ExecId: strings.Repeat("b", 64),
					ImageReferenceSha256: imageDigest, ExpectedLabelsSha256: fact.RequiredLabelsSha256,
					ExecutionNonce: imageDigest, AdapterContractVersion: 1, DatabaseName: "app_000001", RoleName: "app_000001",
					MaxPlaintextBytes: backupformat.MaxAgeSourceBytes, Archive: archive}}
				prepared.Archive = &agentpb.BackupArtifactPrepared_Mysql{Mysql: proto.CloneOf(archive)}
			}
			step.StepDigest = nil
			input.Steps[0], err = SealBackupStepAuthority(step)
			if err != nil {
				t.Fatal(err)
			}
			step = input.Steps[0]
			var raw []byte
			input, raw, attempt.Input, err = CaptureBackupWorkInput(attempt.Input.ID, input)
			clear(raw)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := workexecution.EncodeAgentAttempt(attempt)
			if err != nil {
				t.Fatal(err)
			}
			assignment := &agentpb.BackupWorkAssignment{Attempt: identity, Input: input,
				Sources: []*agentpb.BackupStepResume{{StepId: step.StepId, ExecutionId: step.ExecutionId,
					Operation: &agentpb.BackupStepResume_Capture{Capture: &agentpb.BackupCaptureResume{
						Phase:  agentpb.BackupCapturePhase_BACKUP_CAPTURE_PHASE_CAPTURING,
						Cursor: &agentpb.BackupCaptureCursor{ObjectAttempt: 1}}}}}}
			for _, request := range []*agentpb.BackupWorkCheckpointRequest{observed, dump} {
				current := assignment.Sources[0].GetCapture()
				request.Attempt, request.CheckpointSequence = identity, current.CheckpointSequence+1
				request.PrecedingCheckpoint = proto.CloneOf(current.PrecedingCheckpoint)
				next, err := AdvanceBackupDatabaseCapture(assignment, request, attempt.Executor)
				if err != nil {
					t.Fatal(err)
				}
				next.GetCapture().PrecedingCheckpoint = &agentpb.CheckpointFence{
					DedupeKeyModRevision: int64(
						request.CheckpointSequence,
					), AuthorityDigest: bytes.Clone(step.StepDigest)}
				assignment.Sources[0] = next
			}
			request := &agentpb.BackupWorkCheckpointRequest{Attempt: identity, CheckpointSequence: 3,
				PrecedingCheckpoint: proto.CloneOf(assignment.Sources[0].GetCapture().PrecedingCheckpoint),
				Checkpoint:          &agentpb.BackupWorkCheckpointRequest_ArtifactPrepared{ArtifactPrepared: prepared}}
			next, err := AdvanceBackupDatabaseCapture(assignment, request, attempt.Executor)
			if err != nil || next.GetCapture().GetArtifactPrepared() == nil {
				t.Fatalf("matching native preparation rejected: %v", err)
			}
			if family == "postgres" {
				prepared.GetPostgres().SourceServerVersion = "16.10"
			} else {
				prepared.GetMysql().SourceServerVersion = "8.4.6"
			}
			if _, err := AdvanceBackupDatabaseCapture(assignment, request, attempt.Executor); err == nil {
				t.Fatal("native artifact preparation relabelled the captured dump")
			}
		})
	}
}

func archiveTestRelease(t *testing.T) []byte {
	t.Helper()
	digest := strings.Repeat("a", 64)
	profile := postgres16protocol.ManagedLaunchProfileSHA256()
	image := "registry.example.test/postgres-tools@sha256:" + digest
	index := postgres16protocol.ManagedReleaseIndex{Schema: 1, Image: image,
		DatabaseImage: "docker.io/library/postgres:16-alpine@sha256:" + digest,
		Images: []postgres16protocol.ManagedReleaseImage{{RepositoryDigest: image, ImageID: "sha256:" + digest,
			Manifest: postgres16protocol.ManagedReleaseManifest{
				Schema:              1,
				OS:                  "linux",
				Architecture:        "amd64",
				PostgreSQLMajor:     16,
				HelperSHA256:        digest,
				GateSHA256:          digest,
				PGDumpSHA256:        digest,
				PGRestoreSHA256:     digest,
				PSQLSHA256:          digest,
				LaunchProfileSHA256: hex.EncodeToString(profile[:]),
				GateSeccompSHA256:   digest,
			}}}}
	raw, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := postgres16protocol.DecodeManagedReleaseIndex(raw); err != nil {
		t.Fatal(err)
	}
	return raw
}
