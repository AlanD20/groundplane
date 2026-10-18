package backupconfiguration

import (
	"context"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type transferSlotKey struct {
	attemptID string
	ordinal   uint32
}

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
	slots    map[transferSlotKey]*captureSlot
	restores map[transferSlotKey]*restoreSlot
}

func NewInbox() *Inbox {
	return &Inbox{slots: make(map[transferSlotKey]*captureSlot), restores: make(map[transferSlotKey]*restoreSlot)}
}

func (inbox *Inbox) Register(assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity) error {
	owned, _, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return err
	}
	bindings := make([]backupconfigtransfer.Binding, 0, len(owned.Input.Steps))
	for ordinal, step := range owned.Input.Steps {
		direction := agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE
		if step.GetRestore().GetConfig() != nil {
			direction = agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE
		} else if step.GetCapture().GetConfig() == nil {
			continue
		}
		binding, err := backupconfigtransfer.Bind(owned, identity, uint32(ordinal), direction)
		if err != nil {
			return err
		}
		bindings = append(bindings, binding)
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	// Validate the entire registration before publishing any slots.
	for _, binding := range bindings {
		key := transferSlotKey{binding.Attempt.ID, binding.SourceOrdinal}
		if current := inbox.slots[key]; current != nil && current.binding != binding {
			return invalidCapture()
		}
		if current := inbox.restores[key]; current != nil && current.binding != binding {
			return invalidCapture()
		}
	}
	for _, binding := range bindings {
		key := transferSlotKey{binding.Attempt.ID, binding.SourceOrdinal}
		if binding.Direction == agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_RESTORE {
			if inbox.restores[key] == nil {
				inbox.restores[key] = &restoreSlot{binding: binding, credits: make(chan *agentpb.BackupConfigCredit, 1), done: make(chan struct{})}
			}
		} else if inbox.slots[key] == nil {
			inbox.slots[key] = &captureSlot{binding: binding, frames: make(chan *agentpb.BackupConfigTransfer, backupconfigtransfer.MetadataCreditRecords),
				confirmations: make(chan *agentpb.BackupConfigCredit, 1), done: make(chan struct{})}
		}
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
	slot := inbox.slots[transferSlotKey{frame.GetAttempt().GetAttemptId(), frame.SourceOrdinal}]
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
	slot := inbox.slots[transferSlotKey{credit.GetAttempt().GetAttemptId(), credit.SourceOrdinal}]
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
	binding backupconfigtransfer.Binding,
	publish func(context.Context, *agentpb.BackupConfigCredit) error,
) (*CaptureStream, error) {
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	slot := inbox.slots[transferSlotKey{binding.Attempt.ID, binding.SourceOrdinal}]
	if slot == nil || slot.binding != binding || publish == nil {
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

func (inbox *Inbox) Release(attemptID string) {
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	for stepID, slot := range inbox.slots {
		if attemptID != "" && slot.binding.Attempt.ID != attemptID {
			continue
		}
		delete(inbox.slots, stepID)
		close(slot.done)
		drainCaptureFrames(slot.frames)
	}
	for stepID, slot := range inbox.restores {
		if attemptID != "" && slot.binding.Attempt.ID != attemptID {
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
