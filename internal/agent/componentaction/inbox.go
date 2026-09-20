package componentaction

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"io"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type Inbox struct {
	mu    sync.Mutex
	tasks map[string]*managedConfigTaskInbox
}

type managedConfigTaskInbox struct {
	assignmentID string
	planHash     [sha256.Size]byte
	steps        map[string]*managedConfigStepInbox
}

type managedConfigStepInbox struct {
	expected *agentpb.ComponentApply
	header   ManagedConfigHeader
	content  []byte
	chunks   uint32
	state    managedConfigTransferState
	err      error
	ready    chan struct{}
}

type managedConfigTransferState uint8

const (
	managedConfigAwaitingHeader managedConfigTransferState = iota
	managedConfigReceiving
	managedConfigComplete
	managedConfigFailed
	managedConfigConsumed
)

func NewInbox() *Inbox {
	return &Inbox{tasks: make(map[string]*managedConfigTaskInbox)}
}

func (inbox *Inbox) Register(assignment taskassignment.Assignment) error {
	if inbox == nil || assignment.Plan == nil {
		return errs.New(errs.KindInternal, "agent: managed-config inbox registration is invalid")
	}
	steps := make(map[string]*managedConfigStepInbox)
	for _, step := range assignment.Plan.GetSteps() {
		action := step.GetComponentApply()
		if action == nil || !action.GetManagedConfigContent() ||
			assignment.Plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
			continue
		}
		steps[step.GetStepId()] = &managedConfigStepInbox{
			expected: proto.Clone(action).(*agentpb.ComponentApply),
			ready:    make(chan struct{}),
		}
	}
	if len(steps) == 0 {
		return nil
	}
	task := &managedConfigTaskInbox{
		assignmentID: assignment.AssignmentID,
		planHash:     taskassignment.PlanDigest(assignment.Plan),
		steps:        steps,
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if _, exists := inbox.tasks[assignment.TaskID]; exists {
		return errs.New(errs.KindInternal, "agent: managed-config task was registered twice")
	}
	inbox.tasks[assignment.TaskID] = task
	return nil
}

func (inbox *Inbox) Accept(ctx context.Context, transfer *agentpb.ManagedConfigTransfer) error {
	if ctx == nil || inbox == nil || transfer == nil {
		return errs.New(errs.KindInternal, "agent: managed-config transfer is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[transfer.GetTaskId()]
	if task == nil || transfer.GetAssignmentId() != task.assignmentID ||
		len(transfer.GetPlanHash()) != sha256.Size ||
		subtle.ConstantTimeCompare(transfer.GetPlanHash(), task.planHash[:]) != 1 {
		return errs.New(errs.KindInternal, "agent: managed-config transfer correlation is invalid")
	}
	step := task.steps[transfer.GetStepId()]
	if step == nil {
		return errs.New(errs.KindInternal, "agent: managed-config transfer step is unknown")
	}
	if step.state == managedConfigComplete || step.state == managedConfigConsumed || step.state == managedConfigFailed {
		return inbox.failStep(step, "agent: managed-config transfer contains an extra record")
	}
	switch record := transfer.GetRecord().(type) {
	case *agentpb.ManagedConfigTransfer_Header:
		return inbox.acceptHeader(step, record.Header)
	case *agentpb.ManagedConfigTransfer_Chunk:
		return inbox.acceptChunk(step, record.Chunk)
	case *agentpb.ManagedConfigTransfer_End:
		return inbox.acceptEnd(step, record.End)
	default:
		return inbox.failStep(step, "agent: managed-config transfer record is empty")
	}
}

func (inbox *Inbox) acceptHeader(
	step *managedConfigStepInbox,
	header *agentpb.ManagedConfigTransferHeader,
) error {
	if step.state != managedConfigAwaitingHeader || header == nil ||
		header.GetArtifactId() != step.expected.GetArtifactId() ||
		!managedconfig.ValidMediaType(header.GetMediaType()) || header.GetLength() == 0 ||
		header.GetLength() > managedconfig.MaximumArtifactBytes || len(header.GetSha256()) != sha256.Size ||
		subtle.ConstantTimeCompare(header.GetSha256(), step.expected.GetArtifactDigest()) != 1 {
		return inbox.failStep(step, "agent: managed-config transfer header is invalid")
	}
	var digest [sha256.Size]byte
	copy(digest[:], header.GetSha256())
	step.header = ManagedConfigHeader{
		ArtifactID: header.GetArtifactId(), MediaType: header.GetMediaType(),
		Length: header.GetLength(), Digest: digest,
	}
	step.content = make([]byte, 0, int(header.GetLength()))
	step.state = managedConfigReceiving
	return nil
}

func (inbox *Inbox) acceptChunk(
	step *managedConfigStepInbox,
	chunk *agentpb.ManagedConfigTransferChunk,
) error {
	if step.state != managedConfigReceiving || chunk == nil || chunk.GetSequence() != step.chunks+1 ||
		len(chunk.GetContent()) == 0 || len(chunk.GetContent()) > managedconfig.MaximumChunkBytes ||
		uint64(len(step.content)+len(chunk.GetContent())) > step.header.Length {
		return inbox.failStep(step, "agent: managed-config transfer chunk is invalid")
	}
	step.content = append(step.content, chunk.GetContent()...)
	step.chunks++
	return nil
}

func (inbox *Inbox) acceptEnd(
	step *managedConfigStepInbox,
	end *agentpb.ManagedConfigTransferEnd,
) error {
	if step.state != managedConfigReceiving || end == nil || end.GetChunkCount() != step.chunks ||
		uint64(len(step.content)) != step.header.Length {
		return inbox.failStep(step, "agent: managed-config transfer end is invalid")
	}
	digest := sha256.Sum256(step.content)
	if subtle.ConstantTimeCompare(digest[:], step.header.Digest[:]) != 1 {
		return inbox.failStep(step, "agent: managed-config transfer digest is invalid")
	}
	step.state = managedConfigComplete
	close(step.ready)
	return nil
}

func (inbox *Inbox) Take(
	ctx context.Context,
	taskID string,
	stepID string,
) (ManagedConfigPayload, error) {
	if ctx == nil || inbox == nil {
		return ManagedConfigPayload{}, errs.New(errs.KindInternal, "agent: managed-config take is invalid")
	}
	inbox.mu.Lock()
	task := inbox.tasks[taskID]
	var step *managedConfigStepInbox
	if task != nil {
		step = task.steps[stepID]
	}
	if step == nil {
		inbox.mu.Unlock()
		return ManagedConfigPayload{}, errs.New(errs.KindInternal, "agent: managed-config payload is unknown")
	}
	ready := step.ready
	inbox.mu.Unlock()
	select {
	case <-ctx.Done():
		inbox.Release(taskID)
		return ManagedConfigPayload{}, ctx.Err()
	case <-ready:
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if step.err != nil || step.state != managedConfigComplete {
		if step.err != nil {
			return ManagedConfigPayload{}, step.err
		}
		return ManagedConfigPayload{}, errs.New(errs.KindInternal, "agent: managed-config payload is incomplete")
	}
	source := &ownedManagedConfigSource{content: step.content, reader: bytes.NewReader(step.content)}
	step.content = nil
	step.state = managedConfigConsumed
	delete(task.steps, stepID)
	if len(task.steps) == 0 {
		delete(inbox.tasks, taskID)
	}
	return ManagedConfigPayload{Header: step.header, Source: source}, nil
}

func (inbox *Inbox) failStep(step *managedConfigStepInbox, message string) error {
	err := errs.New(errs.KindInternal, message)
	if step.state != managedConfigFailed && step.state != managedConfigConsumed {
		clear(step.content)
		step.content = nil
		step.err = err
		if step.state != managedConfigComplete {
			close(step.ready)
		}
		step.state = managedConfigFailed
	}
	return err
}

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
		if step.state == managedConfigAwaitingHeader || step.state == managedConfigReceiving {
			inbox.failStep(step, "agent: managed-config transfer was interrupted")
		} else {
			clear(step.content)
			step.content = nil
		}
	}
	delete(inbox.tasks, taskID)
}

type ownedManagedConfigSource struct {
	mu      sync.Mutex
	content []byte
	reader  *bytes.Reader
	closed  bool
}

func (source *ownedManagedConfigSource) Read(destination []byte) (int, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return 0, io.EOF
	}
	return source.reader.Read(destination)
}

func (source *ownedManagedConfigSource) Close() error {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return nil
	}
	clear(source.content)
	source.content = nil
	source.reader = bytes.NewReader(nil)
	source.closed = true
	return nil
}

func CloseSourceWithError(source io.ReadCloser, message string) error {
	if source == nil {
		return errs.New(errs.KindInternal, message)
	}
	if err := source.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return errs.New(errs.KindInternal, message)
}
