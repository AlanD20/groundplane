package backupconfiguration

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type captureSlot struct {
	binding       backupconfigtransfer.Binding
	frames        chan *agentpb.BackupConfigTransfer
	confirmations chan *agentpb.BackupConfigCredit
	done          chan struct{}
	mu            sync.Mutex
	awaiting      *agentpb.BackupConfigCredit
}

// Inbox bounds incoming plaintext to one granted transfer window per reserved
// assignment. The receive/control loop never waits for disk or worker progress.
type Inbox struct {
	mu       sync.Mutex
	slots    map[string]*captureSlot
	restores map[string]*restoreSlot
}

func NewInbox() *Inbox {
	return &Inbox{slots: make(map[string]*captureSlot), restores: make(map[string]*restoreSlot)}
}

func (inbox *Inbox) Register(authority *agentpb.BackupTaskAuthority) error {
	if authority == nil {
		return nil
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	for _, step := range authority.Steps {
		if step.GetRestore().GetConfig() != nil {
			if err := inbox.registerRestore(authority, step); err != nil {
				return err
			}
			continue
		}
		if step.GetCapture().GetConfig() == nil {
			continue
		}
		binding := backupconfigtransfer.Binding{TaskID: authority.TaskId, AssignmentID: authority.AssignmentId,
			StepID: step.StepId, ExecutionID: step.ExecutionId, TransferID: step.ExecutionId,
			Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE}
		if err := binding.Validate(); err != nil {
			return err
		}
		if current := inbox.slots[step.StepId]; current != nil {
			if current.binding != binding {
				return invalidCapture()
			}
			continue
		}
		inbox.slots[step.StepId] = &captureSlot{binding: binding,
			frames:        make(chan *agentpb.BackupConfigTransfer, backupconfigtransfer.MetadataCreditRecords),
			confirmations: make(chan *agentpb.BackupConfigCredit, 1), done: make(chan struct{})}
	}
	return nil
}

func (inbox *Inbox) AcceptFrame(ctx context.Context, frame *agentpb.BackupConfigTransfer) error {
	if ctx == nil || frame == nil {
		return invalidCapture()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	slot := inbox.slots[frame.StepId]
	if slot == nil {
		return invalidCapture()
	}
	owned, err := backupconfigtransfer.ValidateFrame(slot.binding, frame)
	if err != nil {
		return err
	}
	select {
	case slot.frames <- owned:
		return nil
	default:
		backupconfigtransfer.ClearFrame(owned)
		return errs.New(errs.KindStateConflict, "Config transfer exceeds its bounded receiver inbox")
	}
}

func (inbox *Inbox) AcceptCredit(ctx context.Context, credit *agentpb.BackupConfigCredit) error {
	if ctx == nil || credit == nil {
		return invalidCapture()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if credit.Direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
		return inbox.acceptRestoreCredit(credit)
	}
	slot := inbox.slots[credit.StepId]
	if slot == nil {
		return invalidCapture()
	}
	owned, err := backupconfigtransfer.ValidateCredit(slot.binding, credit)
	if err != nil {
		return err
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.awaiting == nil || !proto.Equal(slot.awaiting, owned) {
		return invalidCapture()
	}
	select {
	case slot.confirmations <- owned:
		return nil
	default:
		return invalidCapture()
	}
}

// Stream binds a worker's credit publication port to its already reserved slot.
func (inbox *Inbox) Stream(
	taskID, assignmentID, stepID string,
	publish func(context.Context, *agentpb.BackupConfigCredit) error,
) (*CaptureStream, error) {
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	slot := inbox.slots[stepID]
	if slot == nil || slot.binding.TaskID != taskID || slot.binding.AssignmentID != assignmentID || publish == nil {
		return nil, invalidCapture()
	}
	return &CaptureStream{slot: slot, publish: publish}, nil
}

type CaptureStream struct {
	slot    *captureSlot
	publish func(context.Context, *agentpb.BackupConfigCredit) error
}

func (stream *CaptureStream) Binding() backupconfigtransfer.Binding { return stream.slot.binding }

func (stream *CaptureStream) ReadFrame(ctx context.Context) (*agentpb.BackupConfigTransfer, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-stream.slot.done:
		return nil, invalidCapture()
	case frame := <-stream.slot.frames:
		return frame, nil
	}
}

func (stream *CaptureStream) CommitCredit(ctx context.Context, credit *agentpb.BackupConfigCredit) error {
	owned, err := backupconfigtransfer.ValidateCredit(stream.slot.binding, credit)
	if err != nil {
		return err
	}
	stream.slot.mu.Lock()
	if stream.slot.awaiting != nil {
		stream.slot.mu.Unlock()
		return invalidCapture()
	}
	stream.slot.awaiting = owned
	stream.slot.mu.Unlock()
	defer func() { stream.slot.mu.Lock(); stream.slot.awaiting = nil; stream.slot.mu.Unlock() }()
	if err := stream.publish(ctx, owned); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stream.slot.done:
		return invalidCapture()
	case confirmed := <-stream.slot.confirmations:
		if !proto.Equal(owned, confirmed) {
			return invalidCapture()
		}
		return nil
	}
}

func (inbox *Inbox) Release(taskID string) {
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	for stepID, slot := range inbox.slots {
		if taskID != "" && slot.binding.TaskID != taskID {
			continue
		}
		delete(inbox.slots, stepID)
		close(slot.done)
		drainCaptureFrames(slot.frames)
	}
	for stepID, slot := range inbox.restores {
		if taskID != "" && slot.binding.TaskID != taskID {
			continue
		}
		delete(inbox.restores, stepID)
		close(slot.done)
	}
}

func drainCaptureFrames(frames <-chan *agentpb.BackupConfigTransfer) {
	for {
		select {
		case frame := <-frames:
			backupconfigtransfer.ClearFrame(frame)
		default:
			return
		}
	}
}
