package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (p *WorkerPool) CheckpointBackup(
	ctx context.Context,
	request *agentpb.BackupCheckpointRequest,
) error {
	if ctx == nil || p == nil || p.backupCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Backup checkpoint transport is not configured")
	}
	validated, err := executionplan.ValidateBackupCheckpointRequest(request, request.GetSequence())
	if err != nil {
		return err
	}
	acknowledged, abandon, err := p.backupCheckpoints.Register(validated)
	if err != nil {
		return err
	}
	select {
	case p.outputs <- WorkerOutput{
		BackupCheckpoint: proto.Clone(validated).(*agentpb.BackupCheckpointRequest),
	}:
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
	select {
	case <-acknowledged:
		return nil
	case <-ctx.Done():
		abandon()
		return ctx.Err()
	}
}

func (p *WorkerPool) AcceptBackupCheckpointAck(ack *agentpb.BackupCheckpointAck) error {
	if p == nil || p.backupCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Backup checkpoint transport is not configured")
	}
	return p.backupCheckpoints.Accept(ack)
}
