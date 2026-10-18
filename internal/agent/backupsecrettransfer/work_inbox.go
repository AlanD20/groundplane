package backupsecrettransfer

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// WorkInbox owns only transient credentials for accepted native attempts.
// Release is required on disconnect, termination and completion, even when no
// consumer has begun waiting. Credentials are never restored from a journal.
type WorkInbox struct {
	mu       sync.Mutex
	attempts map[string]*workSecrets
}

type workSecrets struct {
	input    *agentpb.BackupWorkInput
	attempt  *agentpb.WorkAttemptIdentity
	recovery bool
	identity workexecution.Identity
	deadline time.Time
	slots    map[uint32]map[agentpb.BackupSecretSlotPurpose]*workSecretSlot
}

type workSecretSlot struct {
	validator *executionplan.BackupWorkSecretValidator
	content   []byte
	ready     chan struct{}
	state     backupSecretSlotState
	err       error
}

type backupSecretSlotState uint8

const (
	backupSecretAwaitingHeader backupSecretSlotState = iota
	backupSecretReceiving
	backupSecretComplete
	backupSecretFailed
	backupSecretConsumed
)

func NewWorkInbox() *WorkInbox {
	return &WorkInbox{attempts: make(map[string]*workSecrets)}
}

func (inbox *WorkInbox) Register(assignment *agentpb.BackupWorkAssignment, identity workexecution.Identity) error {
	if inbox == nil {
		return errs.New(errs.KindInternal, "Backup credential inbox is unavailable")
	}
	owned, attempt, err := executionplan.OpenBackupWorkAssignment(assignment, identity)
	if err != nil {
		return err
	}
	return inbox.register(owned.Input, owned.Attempt, attempt, identity, false)
}

func (inbox *WorkInbox) RegisterRecovery(assignment *agentpb.BackupRecoveryAssignment, identity workexecution.Identity) error {
	owned, attempt, _, err := executionplan.OpenBackupRecoveryAssignment(assignment, identity)
	if err != nil {
		return err
	}
	if owned.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE ||
		owned.RestoreMode != agentpb.BackupRestoreRecoveryMode_BACKUP_RESTORE_RECOVERY_MODE_LIVE_MUTATION {
		return errs.New(errs.KindStateConflict, "Credentials require active Restore recovery")
	}
	return inbox.register(owned.Input, owned.Attempt, attempt, identity, true)
}

func (inbox *WorkInbox) register(input *agentpb.BackupWorkInput, wire *agentpb.WorkAttemptIdentity,
	attempt workexecution.Attempt, identity workexecution.Identity, recovery bool,
) error {
	if inbox == nil {
		return errs.New(errs.KindInternal, "Backup credential inbox is unavailable")
	}
	if !time.Now().Before(attempt.Deadline) {
		return errs.New(errs.KindStateConflict, "Backup credential attempt has expired")
	}
	slots := make(map[uint32]map[agentpb.BackupSecretSlotPurpose]*workSecretSlot, len(input.Steps))
	for ordinal, step := range input.Steps {
		purposes, err := backupsecret.SourcePurposes(step)
		if err != nil {
			return err
		}
		selected := make(map[agentpb.BackupSecretSlotPurpose]*workSecretSlot, len(purposes))
		for _, purpose := range purposes {
			selected[purpose] = &workSecretSlot{ready: make(chan struct{})}
		}
		slots[uint32(ordinal)] = selected
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if _, found := inbox.attempts[attempt.ID]; found {
		return errs.New(errs.KindStateConflict, "Backup credential attempt is already registered")
	}
	inbox.attempts[attempt.ID] = &workSecrets{input: input, attempt: wire, recovery: recovery, identity: identity, deadline: attempt.Deadline, slots: slots}
	return nil
}

func (inbox *WorkInbox) Accept(ctx context.Context, transfer *agentpb.BackupWorkSecretTransfer) error {
	if inbox == nil || ctx == nil || transfer == nil || transfer.Attempt == nil {
		return errs.New(errs.KindValidationFailed, "Backup credential frame is incomplete")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	work, slot, err := inbox.selectLocked(transfer.Attempt, transfer.SourceOrdinal, transfer.Purpose)
	if err != nil {
		return err
	}
	if transfer.GetHeader() != nil {
		if slot.state != backupSecretAwaitingHeader {
			return failWorkSecret(slot, "Backup credential stream received a duplicate header")
		}
		validator, err := executionplan.NewBackupWorkSecretValidator(transfer, work.input, work.identity)
		if err != nil {
			return failWorkSecret(slot, "Backup credential header does not match captured input")
		}
		slot.validator = validator
		slot.content = make([]byte, 0, int(transfer.GetHeader().TotalBytes))
		slot.state = backupSecretReceiving
		return nil
	}
	if slot.state != backupSecretReceiving || slot.validator == nil {
		return failWorkSecret(slot, "Backup credential frame arrived outside its receiving window")
	}
	if err := slot.validator.Accept(transfer); err != nil {
		return failWorkSecret(slot, "Backup credential frame has invalid ordering or identity")
	}
	if chunk := transfer.GetChunk(); chunk != nil {
		slot.content = append(slot.content, chunk.Content...)
		return nil
	}
	if !slot.validator.Complete() {
		return failWorkSecret(slot, "Backup credential stream is incomplete")
	}
	slot.state = backupSecretComplete
	close(slot.ready)
	return nil
}

func (inbox *WorkInbox) Consume(ctx context.Context, attempt *agentpb.WorkAttemptIdentity, ordinal uint32,
	purpose agentpb.BackupSecretSlotPurpose, consume func([]byte) error,
) error {
	if inbox == nil || ctx == nil || attempt == nil || consume == nil {
		return errs.New(errs.KindInternal, "Backup credential consumption is incomplete")
	}
	inbox.mu.Lock()
	work, slot, err := inbox.selectLocked(attempt, ordinal, purpose)
	if err != nil {
		inbox.mu.Unlock()
		return err
	}
	deadline := work.sourceDeadline(ordinal)
	inbox.mu.Unlock()
	wait, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	select {
	case <-wait.Done():
		inbox.mu.Lock()
		if inbox.attempts[attempt.AttemptId] == work {
			inbox.releaseLockedWork(attempt.AttemptId)
		}
		inbox.mu.Unlock()
		return wait.Err()
	case <-slot.ready:
	}
	inbox.mu.Lock()
	// Recheck ownership after waiting; release or expiration must not authorize
	// a callback merely because the complete frame arrived first.
	_, current, err := inbox.selectLocked(attempt, ordinal, purpose)
	if err != nil {
		inbox.mu.Unlock()
		return err
	}
	if current != slot || slot.state != backupSecretComplete {
		err := slot.err
		if err == nil {
			err = errs.New(errs.KindStateConflict, "Backup credential slot was already consumed")
		}
		inbox.mu.Unlock()
		return err
	}
	content := slot.content
	slot.content = nil
	slot.state = backupSecretConsumed
	inbox.mu.Unlock()
	defer clear(content)
	if err := wait.Err(); err != nil {
		return err
	}
	return consume(content)
}

func (inbox *WorkInbox) selectLocked(attempt *agentpb.WorkAttemptIdentity, ordinal uint32,
	purpose agentpb.BackupSecretSlotPurpose,
) (*workSecrets, *workSecretSlot, error) {
	work := inbox.attempts[attempt.AttemptId]
	if work == nil || !proto.Equal(work.attempt, attempt) || uint64(ordinal) >= uint64(len(work.input.Steps)) {
		return nil, nil, errs.New(errs.KindStateConflict, "Backup credential attempt or source is not registered")
	}
	if !time.Now().Before(work.sourceDeadline(ordinal)) {
		inbox.releaseLockedWork(attempt.AttemptId)
		return nil, nil, errs.New(errs.KindStateConflict, "Backup credential delivery window expired")
	}
	slot := work.slots[ordinal][purpose]
	if slot == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Backup credential purpose is not registered")
	}
	return work, slot, nil
}

func (work *workSecrets) sourceDeadline(ordinal uint32) time.Time {
	if work.recovery {
		return work.deadline
	}
	return minDeadline(work.deadline, time.Unix(0, int64(work.input.Steps[ordinal].StepDeadlineUnixNano)))
}

func (inbox *WorkInbox) Release(attemptID string) {
	if inbox == nil {
		return
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	inbox.releaseLockedWork(attemptID)
}

func (inbox *WorkInbox) ReleaseAll() {
	if inbox == nil {
		return
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	for id := range inbox.attempts {
		inbox.releaseLockedWork(id)
	}
}

func (inbox *WorkInbox) releaseLockedWork(attemptID string) {
	work := inbox.attempts[attemptID]
	if work == nil {
		return
	}
	for _, slots := range work.slots {
		for _, slot := range slots {
			failWorkSecret(slot, "Backup credential attempt was released")
		}
	}
	delete(inbox.attempts, attemptID)
}

func failWorkSecret(slot *workSecretSlot, message string) error {
	err := errs.New(errs.KindStateConflict, message)
	clear(slot.content)
	slot.content = nil
	if slot.state != backupSecretFailed && slot.state != backupSecretConsumed {
		if slot.state != backupSecretComplete {
			close(slot.ready)
		}
		slot.err = err
		slot.state = backupSecretFailed
	}
	return err
}

func minDeadline(first, second time.Time) time.Time {
	if first.Before(second) {
		return first
	}
	return second
}
