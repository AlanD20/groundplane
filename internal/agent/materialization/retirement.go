package materialization

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"time"
)

// Retire clears all buffered plaintext after worker terminalization while
// retaining enough exact authority to validate transfers already in transport.
func (inbox *Inbox) Retire(taskID string) {
	if inbox == nil {
		return
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[taskID]
	if task == nil {
		return
	}
	task.retired = true
	for stepID, step := range task.steps {
		if step.retired {
			continue
		}
		step.retired = true
		switch step.state {
		case materializationReceiving:
			if step.digest != nil {
				step.digest.Destroy()
			}
			step.digest = entrymaterialization.NewHasher()
			written, err := step.digest.Write(step.content)
			if err != nil || written != len(step.content) {
				inbox.failStep(step, "agent: materialization transfer digest state is invalid")
				continue
			}
			clear(step.content)
			step.content = nil
		case materializationComplete, materializationConsumed:
			inbox.destroyStep(step)
			delete(task.steps, stepID)
		default:
			inbox.destroyStep(step)
		}
	}
	if len(task.steps) == 0 {
		inbox.destroyTask(task)
		delete(inbox.tasks, taskID)
		return
	}
	if !time.Now().Before(task.deadline) {
		inbox.destroyTask(task)
		delete(inbox.tasks, taskID)
		return
	}
	if task.expiry == nil {
		inbox.scheduleExpiry(taskID, task)
	}
}

func (inbox *Inbox) scheduleExpiry(taskID string, task *materializationTaskInbox) {
	assignmentID := task.assignmentID
	planHash := task.planHash
	deadline := task.deadline
	task.expiry = time.AfterFunc(time.Until(deadline), func() {
		inbox.expire(taskID, task, assignmentID, planHash, deadline)
	})
}

func (inbox *Inbox) expire(
	taskID string,
	expected *materializationTaskInbox,
	assignmentID string,
	planHash [sha256.Size]byte,
	deadline time.Time,
) {
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[taskID]
	if task != expected || !task.retired || task.assignmentID != assignmentID || task.planHash != planHash ||
		!task.deadline.Equal(deadline) {
		return
	}
	if time.Now().Before(deadline) {
		task.expiry = nil
		inbox.scheduleExpiry(taskID, task)
		return
	}
	task.expiry = nil
	inbox.destroyTask(task)
	delete(inbox.tasks, taskID)
}

func (inbox *Inbox) destroyStep(step *materializationStepInbox) {
	clear(step.content)
	step.content = nil
	if step.shared != nil {
		if step.shared.refs > 0 {
			step.shared.refs--
		}
		if step.shared.refs == 0 {
			clear(step.shared.content)
			step.shared.content = nil
		}
		step.shared = nil
	}
	if step.digest != nil {
		step.digest.Destroy()
		step.digest = nil
	}
}

func (inbox *Inbox) destroyTask(task *materializationTaskInbox) {
	if task.expiry != nil {
		task.expiry.Stop()
		task.expiry = nil
	}
	for _, step := range task.steps {
		inbox.destroyStep(step)
	}
}

// Release is a hard rollback for assignments that were never admitted.
func (inbox *Inbox) Release(taskID string) {
	if inbox == nil {
		return
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[taskID]
	if task == nil {
		return
	}
	for _, step := range task.steps {
		if step.state == materializationAwaitingHeader || step.state == materializationReceiving {
			inbox.failStep(step, "agent: materialization transfer was interrupted")
		} else {
			inbox.destroyStep(step)
		}
	}
	inbox.destroyTask(task)
	delete(inbox.tasks, taskID)
}
