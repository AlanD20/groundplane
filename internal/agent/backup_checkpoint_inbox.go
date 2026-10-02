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
	_, err := p.commitBackupCheckpoint(ctx, request)
	return err
}

func (p *WorkerPool) commitBackupCheckpoint(
	ctx context.Context,
	request *agentpb.BackupCheckpointRequest,
) (*agentpb.BackupCheckpointAck, error) {
	if ctx == nil || p == nil || p.backupCheckpoints == nil {
		return nil, errs.New(errs.KindInternal, "agent: Backup checkpoint transport is not configured")
	}
	validated, err := executionplan.ValidateBackupCheckpointRequest(request, request.GetCheckpointSequence())
	if err != nil {
		return nil, err
	}
	acknowledged, abandon, err := p.backupCheckpoints.Register(validated)
	if err != nil {
		return nil, err
	}
	select {
	case p.outputs <- WorkerOutput{
		BackupCheckpoint: proto.Clone(validated).(*agentpb.BackupCheckpointRequest),
	}:
	case <-ctx.Done():
		abandon()
		return nil, ctx.Err()
	}
	select {
	case ack := <-acknowledged:
		return ack, nil
	case <-ctx.Done():
		abandon()
		return nil, ctx.Err()
	}
}

func (p *WorkerPool) AcceptBackupCheckpointAck(ack *agentpb.BackupCheckpointAck) error {
	if p == nil || p.backupCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Backup checkpoint transport is not configured")
	}
	return p.backupCheckpoints.Accept(ack)
}
