package agent

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupmysql"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type mysqlStartRecorder struct {
	publisher  *backupStepCheckpoint
	authority  mysqlStepAuthority
	pointID    string
	resume     *agentpb.BackupStepResume
	archive    *agentpb.BackupMySQLArchiveEvidence
	attempted  *bool
	called     bool
	dumpStart  *agentpb.BackupMySQLDumpStart
	applyStart *agentpb.BackupMySQLRestoreApplyStartCheckpoint
}

func (recorder *mysqlStartRecorder) check(start mysql84execution.Start,
	operation mysql84protocol.Operation,
) error {
	if recorder == nil || recorder.publisher == nil || recorder.called ||
		start.Request.Operation != operation || start.Request.Validate() != nil ||
		start.Container.ID == "" || start.ExecID == "" || start.ImageReference != recorder.authority.selection.ImageReference ||
		recorder.resume == nil || recorder.resume.GetCapture().GetMysqlDumpStart() != nil ||
		recorder.resume.GetRestore().GetMysqlApplyStart() != nil {
		return invalidAgentStaging()
	}
	recorder.called = true
	return nil
}

func (recorder *mysqlStartRecorder) RecordDumpStart(ctx context.Context, start mysql84execution.Start) error {
	if recorder == nil {
		return invalidAgentStaging()
	}
	if _, err := backupmysql.FromWire(recorder.archive); err != nil {
		return invalidAgentStaging()
	}
	if recorder.check(start, mysql84protocol.OperationDump) != nil ||
		start.Request.Database != recorder.authority.database || start.Request.Role != recorder.authority.role ||
		start.Request.MaximumBytes != recorder.authority.maxPlaintext {
		return invalidAgentStaging()
	}
	request := &agentpb.BackupCheckpointRequest{Checkpoint: &agentpb.BackupCheckpointRequest_MysqlDumpStart{
		MysqlDumpStart: &agentpb.BackupMySQLDumpStart{PointId: recorder.pointID,
			ExecutionNonce: append([]byte(nil), start.Request.Nonce[:]...), ContainerId: start.Container.ID,
			ExecId: start.ExecID, ImageReferenceSha256: append([]byte(nil), recorder.authority.imageRefSHA...),
			ExpectedLabelsSha256:   append([]byte(nil), recorder.authority.labelsSHA256...),
			AdapterContractVersion: mysql84protocol.AdapterContractVersion,
			DatabaseName:           recorder.authority.database, RoleName: recorder.authority.role,
			MaxPlaintextBytes: recorder.authority.maxPlaintext, Archive: proto.CloneOf(recorder.archive)}}}
	if err := recorder.publisher.publish(ctx, request); err != nil {
		return err
	}
	recorder.dumpStart = request.GetMysqlDumpStart()
	return nil
}

func (recorder *mysqlStartRecorder) RecordRestoreApplyStart(ctx context.Context,
	start mysql84execution.Start,
) error {
	if recorder == nil || recorder.publisher == nil || recorder.publisher.step == nil {
		return invalidAgentStaging()
	}
	restore := recorder.publisher.step.GetRestore()
	if restore == nil || restore.ExpectedEvidence == nil ||
		recorder.check(start, mysql84protocol.OperationRestoreApply) != nil ||
		start.Request.Database != recorder.authority.database || start.Request.Role != recorder.authority.role ||
		start.Request.SourceSize != restore.ExpectedEvidence.SourceSizeBytes ||
		!bytes.Equal(start.Request.SourceSHA256[:], restore.ExpectedEvidence.SourceSha256) {
		return invalidAgentStaging()
	}
	if recorder.attempted != nil {
		*recorder.attempted = true
	}
	request := &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_MysqlRestoreApplyStart{
			MysqlRestoreApplyStart: &agentpb.BackupMySQLRestoreApplyStartCheckpoint{PointId: recorder.pointID,
				ExecutionNonce: append([]byte(nil), start.Request.Nonce[:]...), ContainerId: start.Container.ID,
				ExecId: start.ExecID, ImageReferenceSha256: append([]byte(nil), recorder.authority.imageRefSHA...),
				ExpectedLabelsSha256: append([]byte(nil), recorder.authority.labelsSHA256...),
				SourceSizeBytes:      start.Request.SourceSize,
				SourceSha256:         append([]byte(nil), start.Request.SourceSHA256[:]...)},
		},
	}
	if err := recorder.publisher.publish(ctx, request); err != nil {
		return err
	}
	recorder.applyStart = request.GetMysqlRestoreApplyStart()
	return nil
}
