package agent

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type taskEventAckKey struct {
	TaskID, AssignmentID, StepID string
	PlanHash                     PlanHash
	ExecutionEpoch               uint32
	Ordinal                      uint64
	State                        TaskProgressState
}

type taskEventReceipt struct {
	waiter chan struct{}
	ack    *agentpb.TaskEventAck
}

type taskEventAckInbox struct {
	mu       sync.Mutex
	receipts map[taskEventAckKey]*taskEventReceipt
}

func taskEventAckKeyForProgress(progress TaskProgress) taskEventAckKey {
	return taskEventAckKey{
		TaskID: progress.TaskID, AssignmentID: progress.AssignmentID, StepID: progress.StepID,
		PlanHash: progress.PlanHash, ExecutionEpoch: progress.ExecutionEpoch, Ordinal: progress.Ordinal,
		State: progress.State,
	}
}

func (p *WorkerPool) registerTaskEventAck(key taskEventAckKey, waiter chan struct{}) (*taskEventReceipt, bool) {
	receipt := &taskEventReceipt{waiter: waiter}
	p.taskEventAcks.mu.Lock()
	defer p.taskEventAcks.mu.Unlock()
	if _, exists := p.taskEventAcks.receipts[key]; exists {
		return nil, false
	}
	p.taskEventAcks.receipts[key] = receipt
	return receipt, true
}

func (p *WorkerPool) removeTaskEventAck(key taskEventAckKey, receipt *taskEventReceipt) {
	p.taskEventAcks.mu.Lock()
	if current, exists := p.taskEventAcks.receipts[key]; exists && current == receipt {
		delete(p.taskEventAcks.receipts, key)
	}
	p.taskEventAcks.mu.Unlock()
}

func (p *WorkerPool) emitProgress(runCtx context.Context, progress TaskProgress) {
	key := taskEventAckKeyForProgress(progress)
	receipt, registered := p.registerTaskEventAck(key, nil)
	if !registered {
		return
	}
	if !p.publishProgress(runCtx, progress) {
		p.removeTaskEventAck(key, receipt)
	}
}

func (p *WorkerPool) publishProgress(runCtx context.Context, progress TaskProgress) bool {
	owned := progress
	owned.Chunk = append([]byte(nil), progress.Chunk...)
	owned.HealthWait = proto.CloneOf(progress.HealthWait)
	owned.Failure = proto.CloneOf(progress.Failure)
	select {
	case p.outputs <- WorkerOutput{Progress: &owned}:
		return true
	case <-runCtx.Done():
		return false
	}
}

func (p *WorkerPool) emitProgressAndWait(ctx context.Context, progress TaskProgress) error {
	_, err := p.emitProgressForReceipt(ctx, progress)
	return err
}

func (p *WorkerPool) emitProgressForReceipt(ctx context.Context, progress TaskProgress) (*agentpb.TaskEventAck, error) {
	key := taskEventAckKeyForProgress(progress)
	accepted := make(chan struct{})
	receipt, registered := p.registerTaskEventAck(key, accepted)
	if !registered {
		return nil, errs.New(errs.KindStateConflict, "agent: Task event acceptance is already pending")
	}
	// Cancellation stops waiting, not delivery: once published, the exact
	// receipt must survive until the Controller acknowledges it. Removing it
	// here would turn an in-flight acknowledgement into a fatal stale event.
	if !p.publishProgress(ctx, progress) {
		p.removeTaskEventAck(key, receipt)
		return nil, ctx.Err()
	}
	select {
	case <-accepted:
		return receipt.ack, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *WorkerPool) AcceptTaskEventAck(_ context.Context, ack *agentpb.TaskEventAck) error {
	if p == nil || p.taskEventAcks == nil || ack == nil || ack.GetExecutionEpoch() == 0 || ack.GetOrdinal() == 0 ||
		len(ack.GetPlanHash()) != 32 {
		return errs.New(errs.KindValidationFailed, "agent: Controller Task event acknowledgement is invalid")
	}
	if budget := ack.GetRollingBudget(); budget != nil {
		if ack.GetState() != agentpb.TaskState_TASK_STATE_RUNNING || ack.GetOrdinal() != 1 ||
			budget.GetDeadline() == nil || budget.GetDeadline().CheckValid() != nil ||
			budget.GetDrainUntil() != nil && (budget.GetDrainUntil().CheckValid() != nil ||
				!budget.GetDrainUntil().AsTime().Before(budget.GetDeadline().AsTime())) {
			return errs.New(errs.KindValidationFailed, "agent: rolling start acknowledgement has an invalid deadline")
		}
	}
	state := TaskProgressState(0)
	switch ack.GetState() {
	case agentpb.TaskState_TASK_STATE_RUNNING:
		state = TaskProgressRunning
	case agentpb.TaskState_TASK_STATE_COMPLETED:
		state = TaskProgressCompleted
	case agentpb.TaskState_TASK_STATE_FAILED:
		state = TaskProgressFailed
	case agentpb.TaskState_TASK_STATE_TIMED_OUT:
		state = TaskProgressTimedOut
	case agentpb.TaskState_TASK_STATE_ABORTED:
		state = TaskProgressAborted
	default:
		return errs.New(errs.KindValidationFailed, "agent: Controller Task event acknowledgement state is invalid")
	}
	var planHash PlanHash
	copy(planHash[:], ack.GetPlanHash())
	key := taskEventAckKey{
		TaskID: ack.GetTaskId(), AssignmentID: ack.GetAssignmentId(), StepID: ack.GetStepId(),
		PlanHash: planHash, ExecutionEpoch: ack.GetExecutionEpoch(), Ordinal: ack.GetOrdinal(), State: state,
	}
	p.taskEventAcks.mu.Lock()
	receipt, exists := p.taskEventAcks.receipts[key]
	if exists {
		delete(p.taskEventAcks.receipts, key)
	}
	p.taskEventAcks.mu.Unlock()
	if !exists {
		return errs.New(errs.KindStateConflict, "agent: stale Task event acknowledgement")
	}
	if receipt.waiter != nil {
		receipt.ack = proto.CloneOf(ack)
		close(receipt.waiter)
	}
	return nil
}
