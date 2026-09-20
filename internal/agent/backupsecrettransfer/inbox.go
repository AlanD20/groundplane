package backupsecrettransfer

import (
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Inbox struct {
	mu    sync.Mutex
	tasks map[string]*backupSecretTaskInbox
}

type backupSecretTaskInbox struct {
	assignmentID string
	steps        map[string]map[agentpb.BackupSecretSlotPurpose]*backupSecretSlot
}

type backupSecretSlot struct {
	validator *executionplan.BackupSecretSlotValidator
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

func New() *Inbox {
	return &Inbox{tasks: make(map[string]*backupSecretTaskInbox)}
}

func (inbox *Inbox) Register(assignment taskassignment.Assignment) error {
	if inbox == nil || assignment.Plan == nil {
		return errs.New(errs.KindInternal, "agent: Backup secret inbox registration is invalid")
	}
	steps := make(map[string]map[agentpb.BackupSecretSlotPurpose]*backupSecretSlot)
	for _, step := range assignment.Plan.GetSteps() {
		purposes, reserved, err := backupSecretSlotPurposes(step)
		if err != nil {
			return err
		}
		if !reserved {
			continue
		}
		slots := make(map[agentpb.BackupSecretSlotPurpose]*backupSecretSlot, len(purposes))
		for _, purpose := range purposes {
			slots[purpose] = &backupSecretSlot{ready: make(chan struct{})}
		}
		steps[step.GetStepId()] = slots
	}
	if len(steps) == 0 {
		return nil
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if _, exists := inbox.tasks[assignment.TaskID]; exists {
		return errs.New(errs.KindInternal, "agent: Backup secret task was registered twice")
	}
	inbox.tasks[assignment.TaskID] = &backupSecretTaskInbox{
		assignmentID: assignment.AssignmentID,
		steps:        steps,
	}
	return nil
}

func backupSecretSlotPurposes(
	step *agentpb.ExecutionStep,
) ([]agentpb.BackupSecretSlotPurpose, bool, error) {
	if step == nil {
		return nil, false, errs.New(errs.KindInternal, "agent: Backup secret slot step is required")
	}
	if capture := step.GetBackupSourceCapture(); capture != nil {
		if capture.GetEncryption() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE &&
			capture.GetEncryption() != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			return nil, false, errs.New(errs.KindInternal, "agent: Backup source encryption is invalid")
		}
	} else if step.GetBackupArtifactPrune() == nil {
		return nil, false, nil
	}
	return []agentpb.BackupSecretSlotPurpose{
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY,
		agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY,
	}, true, nil
}

func (inbox *Inbox) Accept(
	ctx context.Context,
	transfer *agentpb.BackupSecretSlotTransfer,
) error {
	if ctx == nil || inbox == nil || transfer == nil {
		return errs.New(errs.KindInternal, "agent: Backup secret slot transfer is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[transfer.GetTaskId()]
	if task == nil || task.assignmentID != transfer.GetAssignmentId() {
		return errs.New(errs.KindStateConflict, "agent: Backup secret slot assignment is not reserved")
	}
	slots := task.steps[transfer.GetStepId()]
	slot := slots[transfer.GetPurpose()]
	if slot == nil {
		return errs.New(errs.KindStateConflict, "agent: Backup secret slot purpose is not reserved")
	}
	if header := transfer.GetHeader(); header != nil {
		if slot.state != backupSecretAwaitingHeader {
			return inbox.failSlot(slot, "agent: Backup secret slot contains an extra header")
		}
		validator, err := executionplan.NewBackupSecretSlotValidator(transfer)
		if err != nil {
			return inbox.failSlot(slot, "agent: Backup secret slot header is invalid")
		}
		slot.validator = validator
		slot.content = make([]byte, 0, int(header.GetTotalBytes()))
		slot.state = backupSecretReceiving
		return nil
	}
	if slot.state != backupSecretReceiving || slot.validator == nil {
		return inbox.failSlot(slot, "agent: Backup secret slot record arrived before its header")
	}
	if err := slot.validator.Accept(transfer); err != nil {
		return inbox.failSlot(slot, "agent: Backup secret slot record is invalid")
	}
	if chunk := transfer.GetChunk(); chunk != nil {
		slot.content = append(slot.content, chunk.GetContent()...)
		return nil
	}
	if transfer.GetEnd() == nil || !slot.validator.Complete() {
		return inbox.failSlot(slot, "agent: Backup secret slot end is invalid")
	}
	slot.state = backupSecretComplete
	close(slot.ready)
	return nil
}

func (inbox *Inbox) Consume(
	ctx context.Context,
	taskID string,
	assignmentID string,
	stepID string,
	purpose agentpb.BackupSecretSlotPurpose,
	consume func([]byte) error,
) error {
	if ctx == nil || inbox == nil || consume == nil {
		return errs.New(errs.KindInternal, "agent: Backup secret consume request is invalid")
	}
	inbox.mu.Lock()
	task := inbox.tasks[taskID]
	if task == nil || task.assignmentID != assignmentID {
		inbox.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Backup secret slot assignment is not reserved")
	}
	slots := task.steps[stepID]
	slot := slots[purpose]
	if slot == nil {
		inbox.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Backup secret slot purpose is not reserved")
	}
	ready := slot.ready
	inbox.mu.Unlock()
	select {
	case <-ctx.Done():
		inbox.Release(taskID)
		return ctx.Err()
	case <-ready:
	}
	inbox.mu.Lock()
	if slot.err != nil || slot.state != backupSecretComplete {
		err := slot.err
		inbox.mu.Unlock()
		if err != nil {
			return err
		}
		return errs.New(errs.KindInternal, "agent: Backup secret slot is incomplete")
	}
	content := slot.content
	slot.content = nil
	slot.state = backupSecretConsumed
	delete(slots, purpose)
	if len(slots) == 0 {
		delete(task.steps, stepID)
	}
	if len(task.steps) == 0 {
		delete(inbox.tasks, taskID)
	}
	inbox.mu.Unlock()
	defer clear(content)
	return consume(content)
}

func (inbox *Inbox) Release(taskID string) {
	if inbox == nil {
		return
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	inbox.releaseLocked(taskID)
}

func (inbox *Inbox) ReleaseAll() {
	if inbox == nil {
		return
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	for taskID := range inbox.tasks {
		inbox.releaseLocked(taskID)
	}
}

func (inbox *Inbox) releaseLocked(taskID string) {
	task := inbox.tasks[taskID]
	if task == nil {
		return
	}
	for _, slots := range task.steps {
		for _, slot := range slots {
			clear(slot.content)
			slot.content = nil
			if slot.state != backupSecretFailed && slot.state != backupSecretConsumed {
				slot.err = errs.New(errs.KindStateConflict, "agent: Backup secret slot was released")
				if slot.state != backupSecretComplete {
					close(slot.ready)
				}
			}
			slot.state = backupSecretFailed
		}
	}
	delete(inbox.tasks, taskID)
}

func (inbox *Inbox) failSlot(slot *backupSecretSlot, message string) error {
	err := errs.New(errs.KindInternal, message)
	if slot.state != backupSecretFailed && slot.state != backupSecretConsumed {
		clear(slot.content)
		slot.content = nil
		slot.err = err
		if slot.state != backupSecretComplete {
			close(slot.ready)
		}
		slot.state = backupSecretFailed
	}
	return err
}
