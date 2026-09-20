package materialization

import (
	"bytes"
	"context"
	"crypto/sha256"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"io"
	"sync"
	"time"
)

type Inbox struct {
	mu    sync.Mutex
	tasks map[string]*materializationTaskInbox
}

type materializationTaskInbox struct {
	assignmentID     string
	planHash         [sha256.Size]byte
	renderGeneration uint64
	executionEpoch   uint32
	executionMode    agentpb.TaskExecutionMode
	deadline         time.Time
	recoveryDeadline time.Time
	steps            map[string]*materializationStepInbox
	retired          bool
	expiry           *time.Timer
}

type materializationStepInbox struct {
	expected *agentpb.MaterializeFile
	header   entrymaterialization.Header
	content  []byte
	shared   *sharedMaterializationContent
	group    string
	chunks   uint32
	received uint64
	digest   entrymaterialization.Hasher
	state    materializationTransferState
	retired  bool
	reusable bool
	err      error
	ready    chan struct{}
}

type sharedMaterializationContent struct {
	content []byte
	refs    uint32
}

type materializationTransferState uint8

const (
	materializationAwaitingHeader materializationTransferState = iota
	materializationReceiving
	materializationComplete
	materializationFailed
	materializationConsumed
	maximumMaterializationTransferChunks uint32 = 32
)

type Payload struct {
	Header entrymaterialization.Header
	Source io.ReadCloser
}

func NewInbox() *Inbox {
	return &Inbox{tasks: make(map[string]*materializationTaskInbox)}
}

func (inbox *Inbox) Register(assignment taskassignment.Assignment) error {
	if inbox == nil || assignment.Plan == nil {
		return errs.New(errs.KindInternal, "agent: materialization inbox registration is invalid")
	}
	steps := make(map[string]*materializationStepInbox)
	for _, step := range assignment.Plan.GetSteps() {
		materialization := step.GetMaterializeFile()
		if materialization == nil {
			continue
		}
		pair := executionplan.ConfigurationFilePair(assignment.Plan, step.GetStepId())
		steps[step.GetStepId()] = &materializationStepInbox{
			expected: proto.Clone(materialization).(*agentpb.MaterializeFile),
			reusable: pair != nil,
			group: func() string {
				if pair == nil {
					return ""
				}
				return pair.GetForwardStepId()
			}(),
			ready: make(chan struct{}),
		}
	}
	if len(steps) == 0 {
		return nil
	}
	task := &materializationTaskInbox{
		assignmentID: assignment.AssignmentID,
		planHash: taskassignment.PlanDigest(
			assignment.Plan,
		),
		renderGeneration: assignment.Plan.GetRenderGeneration(),
		executionEpoch:   assignment.ExecutionEpoch,
		executionMode:    assignment.ExecutionMode,
		deadline:         assignment.Deadline,
		recoveryDeadline: assignment.RecoveryDeadline,
		steps:            steps,
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if existing := inbox.tasks[assignment.TaskID]; existing != nil {
		sameAttempt := existing.executionMode == task.executionMode &&
			existing.executionEpoch == task.executionEpoch
		recoveryTransition := existing.executionMode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD &&
			task.executionMode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY &&
			task.executionEpoch == existing.executionEpoch+1
		validDeadline := sameAttempt && task.deadline.Equal(existing.deadline) ||
			recoveryTransition && task.deadline.Equal(existing.recoveryDeadline)
		if !existing.retired || existing.assignmentID != task.assignmentID ||
			existing.planHash != task.planHash || existing.renderGeneration != task.renderGeneration ||
			!validDeadline || !task.recoveryDeadline.Equal(existing.recoveryDeadline) {
			return errs.New(errs.KindInternal, "agent: materialization task was registered twice")
		}
		inbox.destroyTask(existing)
	}
	inbox.tasks[assignment.TaskID] = task
	return nil
}

func (inbox *Inbox) Take(
	ctx context.Context,
	taskID string,
	stepID string,
) (Payload, error) {
	if ctx == nil || inbox == nil {
		return Payload{}, errs.New(errs.KindInternal, "agent: materialization take is invalid")
	}
	inbox.mu.Lock()
	task := inbox.tasks[taskID]
	var step *materializationStepInbox
	if task != nil {
		step = task.steps[stepID]
	}
	if step == nil {
		inbox.mu.Unlock()
		return Payload{}, errs.New(errs.KindInternal, "agent: materialization payload is unknown")
	}
	ready := step.ready
	inbox.mu.Unlock()
	select {
	case <-ctx.Done():
		inbox.Retire(taskID)
		return Payload{}, ctx.Err()
	case <-ready:
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if step.retired {
		if err := ctx.Err(); err != nil {
			return Payload{}, err
		}
		return Payload{}, errs.New(errs.KindInternal, "agent: materialization payload was retired")
	}
	if step.err != nil || step.state != materializationComplete {
		if step.err != nil {
			return Payload{}, step.err
		}
		return Payload{}, errs.New(errs.KindInternal, "agent: materialization payload is incomplete")
	}
	content := step.content
	if step.reusable {
		if step.shared == nil || step.shared.refs == 0 {
			return Payload{}, errs.New(
				errs.KindInternal,
				"agent: reusable materialization payload is invalid",
			)
		}
		content = bytes.Clone(step.shared.content)
	}
	source := &ownedMaterializationSource{content: content, reader: bytes.NewReader(content)}
	if step.reusable {
		return Payload{Header: step.header, Source: source}, nil
	}
	step.content = nil
	step.state = materializationConsumed
	delete(task.steps, stepID)
	if len(task.steps) == 0 {
		delete(inbox.tasks, taskID)
	}
	return Payload{Header: step.header, Source: source}, nil
}
