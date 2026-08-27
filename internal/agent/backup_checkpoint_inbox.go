package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type backupCheckpointKey struct {
	taskID       string
	assignmentID string
	stepID       string
	sequence     uint64
}

type backupCheckpointWaiter struct {
	request   *agentpb.BackupCheckpointRequest
	ack       chan *agentpb.BackupCheckpointAck
	abandoned bool
}

type backupCheckpointInbox struct {
	mu      sync.Mutex
	pending map[backupCheckpointKey]*backupCheckpointWaiter
}

func newBackupCheckpointInbox() *backupCheckpointInbox {
	return &backupCheckpointInbox{pending: make(map[backupCheckpointKey]*backupCheckpointWaiter)}
}

func (inbox *backupCheckpointInbox) Register(
	request *agentpb.BackupCheckpointRequest,
) (<-chan *agentpb.BackupCheckpointAck, func(), error) {
	if inbox == nil || request == nil {
		return nil, nil, errs.New(errs.KindInternal, "agent: Backup checkpoint inbox is not configured")
	}
	key := backupCheckpointRequestKey(request)
	waiter := &backupCheckpointWaiter{
		request: proto.Clone(request).(*agentpb.BackupCheckpointRequest),
		ack:     make(chan *agentpb.BackupCheckpointAck, 1),
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if _, exists := inbox.pending[key]; exists {
		return nil, nil, errs.New(errs.KindStateConflict, "agent: Backup checkpoint is already pending")
	}
	inbox.pending[key] = waiter
	return waiter.ack, func() {
		inbox.mu.Lock()
		defer inbox.mu.Unlock()
		if current := inbox.pending[key]; current == waiter {
			current.abandoned = true
		}
	}, nil
}

func (inbox *backupCheckpointInbox) Accept(ack *agentpb.BackupCheckpointAck) error {
	if inbox == nil || ack == nil {
		return errs.New(errs.KindInternal, "agent: Backup checkpoint acknowledgement is missing")
	}
	key := backupCheckpointAckKey(ack)
	inbox.mu.Lock()
	waiter := inbox.pending[key]
	if waiter == nil {
		inbox.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Backup checkpoint acknowledgement is unexpected")
	}
	if err := executionplan.ValidateBackupCheckpointAck(ack, waiter.request); err != nil {
		inbox.mu.Unlock()
		return err
	}
	delete(inbox.pending, key)
	abandoned := waiter.abandoned
	inbox.mu.Unlock()
	if !abandoned {
		waiter.ack <- proto.Clone(ack).(*agentpb.BackupCheckpointAck)
	}
	return nil
}

func backupCheckpointRequestKey(request *agentpb.BackupCheckpointRequest) backupCheckpointKey {
	return backupCheckpointKey{
		taskID:       request.GetTaskId(),
		assignmentID: request.GetAssignmentId(),
		stepID:       request.GetStepId(),
		sequence:     request.GetSequence(),
	}
}

func backupCheckpointAckKey(ack *agentpb.BackupCheckpointAck) backupCheckpointKey {
	return backupCheckpointKey{
		taskID:       ack.GetTaskId(),
		assignmentID: ack.GetAssignmentId(),
		stepID:       ack.GetStepId(),
		sequence:     ack.GetSequence(),
	}
}

// CheckpointBackup sends one Agent-owned checkpoint through the sole stream
// writer and blocks the operation until the Controller durably acknowledges it.
func (p *WorkerPool) CheckpointBackup(
	ctx context.Context,
	request *agentpb.BackupCheckpointRequest,
) error {
	if ctx == nil || p == nil || p.backupCheckpoints == nil {
		return errs.New(errs.KindInternal, "agent: Backup checkpoint transport is not configured")
	}
	if err := executionplan.ValidateBackupCheckpointRequest(request, request.GetSequence()); err != nil {
		return err
	}
	acknowledged, abandon, err := p.backupCheckpoints.Register(request)
	if err != nil {
		return err
	}
	select {
	case p.outputs <- WorkerOutput{
		BackupCheckpoint: proto.Clone(request).(*agentpb.BackupCheckpointRequest),
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
