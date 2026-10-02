package agent

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The Docker port calls this immediately before the start-bearing ExecAttach.
// backupStepCheckpoint.publish returns only after the Controller's exact native
// receipt, and an unknown Ack never grants permission to call ExecAttach.
type postgresStartRecorder struct {
	publisher  *backupStepCheckpoint
	authority  postgresStepAuthority
	pointID    string
	resume     *agentpb.BackupStepResume
	attempted  *bool
	called     bool
	dumpStart  *agentpb.BackupPostgresDumpStart
	applyStart *agentpb.BackupPostgresRestoreApplyStartCheckpoint
}

func (recorder *postgresStartRecorder) check(start postgres16execution.Start,
	operation postgres16protocol.Operation,
) error {
	if recorder == nil || recorder.publisher == nil || recorder.called ||
		start.Request.Operation != operation || start.Request.Validate() != nil ||
		start.Container.ID == "" || start.ExecID == "" || start.ImageID != recorder.authority.imageID ||
		start.ManifestDigest != "sha256:"+hex.EncodeToString(recorder.authority.repositorySHA) ||
		!strings.HasSuffix(start.ImageReference, "@"+start.ManifestDigest) {
		return invalidAgentStaging()
	}
	if recorder.resume == nil || recorder.resume.GetCapture().GetDumpStart() != nil ||
		recorder.resume.GetRestore().GetApplyStart() != nil {
		return invalidAgentStaging()
	}
	recorder.called = true
	return nil
}

func (recorder *postgresStartRecorder) RecordDumpStart(ctx context.Context,
	start postgres16execution.Start,
) error {
	if err := recorder.check(start, postgres16protocol.OperationDump); err != nil ||
		start.Request.Database != recorder.authority.database || start.Request.Role != recorder.authority.role ||
		recorder.authority.maxPlaintext == 0 {
		return invalidAgentStaging()
	}
	request := &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_PostgresDumpStart{
			PostgresDumpStart: &agentpb.BackupPostgresDumpStart{
				PointId: recorder.pointID, ExecutionNonce: append([]byte(nil), start.Request.Nonce[:]...),
				ContainerId: start.Container.ID, ExecId: start.ExecID,
				RepositoryDigest:       append([]byte(nil), recorder.authority.repositorySHA...),
				ExpectedLabelsSha256:   append([]byte(nil), recorder.authority.labelsSHA256...),
				AdapterContractVersion: postgres16protocol.AdapterContractVersion,
				DatabaseName:           recorder.authority.database, RoleName: recorder.authority.role,
				MaxPlaintextBytes: recorder.authority.maxPlaintext,
			},
		},
	}
	if err := recorder.publisher.publish(ctx, request); err != nil {
		return err
	}
	recorder.dumpStart = request.GetPostgresDumpStart()
	return nil
}

func (recorder *postgresStartRecorder) RecordRestoreApplyStart(ctx context.Context,
	start postgres16execution.Start,
) error {
	if recorder == nil || recorder.publisher == nil || recorder.publisher.step == nil {
		return invalidAgentStaging()
	}
	restore := recorder.publisher.step.GetRestore()
	if restore == nil || restore.ExpectedEvidence == nil ||
		recorder.check(start, postgres16protocol.OperationRestoreApply) != nil ||
		start.Request.Database != recorder.authority.database || start.Request.Role != recorder.authority.role ||
		start.Request.SourceSize == 0 || start.Request.SourceSHA256 == (postgres16protocol.Digest{}) ||
		start.Request.SourceSize != restore.ExpectedEvidence.SourceSizeBytes ||
		!bytes.Equal(start.Request.SourceSHA256[:], restore.ExpectedEvidence.SourceSha256) {
		return invalidAgentStaging()
	}
	// A lost native Ack can mean the destructive start was durably recorded.
	// Keep the Restore mutation flag even when publish returns an error.
	if recorder.attempted != nil {
		*recorder.attempted = true
	}
	request := &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_PostgresRestoreApplyStart{
			PostgresRestoreApplyStart: &agentpb.BackupPostgresRestoreApplyStartCheckpoint{
				PointId: recorder.pointID, ExecutionNonce: append([]byte(nil), start.Request.Nonce[:]...),
				ContainerId: start.Container.ID, ExecId: start.ExecID,
				RepositoryDigest:     append([]byte(nil), recorder.authority.repositorySHA...),
				ExpectedLabelsSha256: append([]byte(nil), recorder.authority.labelsSHA256...),
				SourceSizeBytes:      start.Request.SourceSize,
				SourceSha256:         append([]byte(nil), start.Request.SourceSHA256[:]...),
			},
		},
	}
	if err := recorder.publisher.publish(ctx, request); err != nil {
		return err
	}
	recorder.applyStart = request.GetPostgresRestoreApplyStart()
	return nil
}
