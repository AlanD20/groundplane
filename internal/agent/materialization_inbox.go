package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type materializationInbox struct {
	mu    sync.Mutex
	tasks map[string]*materializationTaskInbox
}

type materializationTaskInbox struct {
	planHash         [sha256.Size]byte
	renderGeneration uint64
	steps            map[string]*materializationStepInbox
}

type materializationStepInbox struct {
	expected *agentpb.MaterializeFile
	header   entrymaterialization.Header
	content  []byte
	chunks   uint32
	state    materializationTransferState
	err      error
	ready    chan struct{}
}

type materializationTransferState uint8

const (
	materializationAwaitingHeader materializationTransferState = iota
	materializationReceiving
	materializationComplete
	materializationFailed
	materializationConsumed
)

type materializationPayload struct {
	Header entrymaterialization.Header
	Source io.ReadCloser
}

func newMaterializationInbox() *materializationInbox {
	return &materializationInbox{tasks: make(map[string]*materializationTaskInbox)}
}

func (inbox *materializationInbox) Register(assignment Assignment) error {
	if inbox == nil || assignment.Plan == nil {
		return errs.New(errs.KindInternal, "agent: materialization inbox registration is invalid")
	}
	steps := make(map[string]*materializationStepInbox)
	for _, step := range assignment.Plan.GetSteps() {
		materialization := step.GetMaterializeFile()
		if materialization == nil {
			continue
		}
		steps[step.GetStepId()] = &materializationStepInbox{
			expected: proto.Clone(materialization).(*agentpb.MaterializeFile),
			ready:    make(chan struct{}),
		}
	}
	if len(steps) == 0 {
		return nil
	}
	task := &materializationTaskInbox{
		planHash: hashForPlan(assignment.Plan), renderGeneration: assignment.Plan.GetRenderGeneration(), steps: steps,
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if _, exists := inbox.tasks[assignment.TaskID]; exists {
		return errs.New(errs.KindInternal, "agent: materialization task was registered twice")
	}
	inbox.tasks[assignment.TaskID] = task
	return nil
}

func (inbox *materializationInbox) Accept(
	ctx context.Context,
	transfer *agentpb.MaterializationTransfer,
) error {
	if ctx == nil || inbox == nil || transfer == nil {
		return errs.New(errs.KindInternal, "agent: materialization transfer is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[transfer.GetTaskId()]
	if task == nil || len(transfer.GetPlanHash()) != sha256.Size ||
		subtle.ConstantTimeCompare(transfer.GetPlanHash(), task.planHash[:]) != 1 {
		return errs.New(errs.KindInternal, "agent: materialization transfer correlation is invalid")
	}
	step := task.steps[transfer.GetStepId()]
	if step == nil {
		return errs.New(errs.KindInternal, "agent: materialization transfer step is unknown")
	}
	if step.state == materializationComplete || step.state == materializationConsumed ||
		step.state == materializationFailed {
		return inbox.failStep(step, "agent: materialization transfer contains an extra record")
	}
	switch record := transfer.GetRecord().(type) {
	case *agentpb.MaterializationTransfer_Header:
		return inbox.acceptHeader(transfer, task, step, record.Header)
	case *agentpb.MaterializationTransfer_Chunk:
		return inbox.acceptChunk(step, record.Chunk)
	case *agentpb.MaterializationTransfer_End:
		return inbox.acceptEnd(step, record.End)
	default:
		return inbox.failStep(step, "agent: materialization transfer record is empty")
	}
}

func (inbox *materializationInbox) acceptHeader(
	transfer *agentpb.MaterializationTransfer,
	task *materializationTaskInbox,
	step *materializationStepInbox,
	header *agentpb.MaterializationTransferHeader,
) error {
	if step.state != materializationAwaitingHeader || header == nil ||
		!materializationHeaderMatches(step.expected, header) {
		return inbox.failStep(step, "agent: materialization transfer header is invalid")
	}
	var digest entrymaterialization.Digest
	copy(digest[:], header.GetSha256())
	outputKind, err := transferOutputKind(header.GetOutputKind())
	if err != nil {
		return inbox.failStep(step, "agent: materialization transfer output kind is invalid")
	}
	validated, err := entrymaterialization.NewHeader(entrymaterialization.HeaderSpec{
		TaskID: transfer.GetTaskId(), StepID: transfer.GetStepId(), EnvironmentID: header.GetEnvironmentId(),
		Generation: header.GetRenderGeneration(), Destination: header.GetDestination(), ServiceID: header.GetServiceId(),
		OutputKind: outputKind, UID: header.GetUid(), GID: header.GetGid(),
		Mode: entrymaterialization.Mode(header.GetMode()), Length: header.GetLength(), Digest: digest,
	})
	if err != nil || header.GetRenderGeneration() != task.renderGeneration {
		return inbox.failStep(step, "agent: materialization transfer header policy is invalid")
	}
	step.header = validated
	step.content = make([]byte, 0, int(header.GetLength()))
	step.state = materializationReceiving
	return nil
}

func (inbox *materializationInbox) acceptChunk(
	step *materializationStepInbox,
	chunk *agentpb.MaterializationTransferChunk,
) error {
	if step.state != materializationReceiving || chunk == nil || chunk.GetSequence() != step.chunks+1 ||
		len(chunk.GetContent()) == 0 || len(chunk.GetContent()) > entrymaterialization.MaximumChunkBytes ||
		uint64(len(step.content)+len(chunk.GetContent())) > step.expected.GetLength() {
		return inbox.failStep(step, "agent: materialization transfer chunk is invalid")
	}
	step.content = append(step.content, chunk.GetContent()...)
	step.chunks++
	return nil
}

func (inbox *materializationInbox) acceptEnd(
	step *materializationStepInbox,
	end *agentpb.MaterializationTransferEnd,
) error {
	if step.state != materializationReceiving || end == nil || end.GetChunkCount() != step.chunks ||
		uint64(len(step.content)) != step.expected.GetLength() {
		return inbox.failStep(step, "agent: materialization transfer end is invalid")
	}
	digest := sha256.Sum256(step.content)
	if subtle.ConstantTimeCompare(digest[:], step.expected.GetSha256()) != 1 {
		return inbox.failStep(step, "agent: materialization transfer digest is invalid")
	}
	step.state = materializationComplete
	close(step.ready)
	return nil
}

func materializationHeaderMatches(
	expected *agentpb.MaterializeFile,
	header *agentpb.MaterializationTransferHeader,
) bool {
	return expected != nil && header.GetArtifactId() == expected.GetArtifactId() &&
		header.GetMaterializationId() == expected.GetMaterializationId() &&
		header.GetEnvironmentId() == expected.GetEnvironmentId() &&
		header.GetDestination() == expected.GetDestination() && header.GetServiceId() == expected.GetServiceId() &&
		header.GetOutputKind() == expected.GetOutputKind() && header.GetUid() == expected.GetUid() &&
		header.GetGid() == expected.GetGid() && header.GetMode() == expected.GetMode() &&
		header.GetLength() == expected.GetLength() && bytes.Equal(header.GetSha256(), expected.GetSha256())
}

func transferOutputKind(value agentpb.MaterializationOutputKind) (entrymaterialization.OutputKind, error) {
	switch value {
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_GENERATED_ENV:
		return entrymaterialization.OutputGeneratedEnv, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE:
		return entrymaterialization.OutputPlainFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_SECRET_FILE:
		return entrymaterialization.OutputSecretFile, nil
	default:
		return 0, errs.New(errs.KindInternal, "agent: materialization output kind is invalid")
	}
}

func (inbox *materializationInbox) failStep(step *materializationStepInbox, message string) error {
	err := errs.New(errs.KindInternal, message)
	if step.state != materializationFailed && step.state != materializationConsumed {
		clear(step.content)
		step.content = nil
		step.err = err
		if step.state != materializationComplete {
			close(step.ready)
		}
		step.state = materializationFailed
	}
	return err
}

func (inbox *materializationInbox) Take(
	ctx context.Context,
	taskID string,
	stepID string,
) (materializationPayload, error) {
	if ctx == nil || inbox == nil {
		return materializationPayload{}, errs.New(errs.KindInternal, "agent: materialization take is invalid")
	}
	inbox.mu.Lock()
	task := inbox.tasks[taskID]
	var step *materializationStepInbox
	if task != nil {
		step = task.steps[stepID]
	}
	if step == nil {
		inbox.mu.Unlock()
		return materializationPayload{}, errs.New(errs.KindInternal, "agent: materialization payload is unknown")
	}
	ready := step.ready
	inbox.mu.Unlock()
	select {
	case <-ctx.Done():
		inbox.Release(taskID)
		return materializationPayload{}, ctx.Err()
	case <-ready:
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if step.err != nil || step.state != materializationComplete {
		if step.err != nil {
			return materializationPayload{}, step.err
		}
		return materializationPayload{}, errs.New(errs.KindInternal, "agent: materialization payload is incomplete")
	}
	source := &ownedMaterializationSource{
		content: step.content,
		reader:  bytes.NewReader(step.content),
	}
	step.content = nil
	step.state = materializationConsumed
	delete(task.steps, stepID)
	if len(task.steps) == 0 {
		delete(inbox.tasks, taskID)
	}
	return materializationPayload{Header: step.header, Source: source}, nil
}

func (inbox *materializationInbox) Release(taskID string) {
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
			clear(step.content)
			step.content = nil
		}
	}
	delete(inbox.tasks, taskID)
}

type ownedMaterializationSource struct {
	mu      sync.Mutex
	content []byte
	reader  *bytes.Reader
	closed  bool
}

func (source *ownedMaterializationSource) Read(destination []byte) (int, error) {
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.closed {
		return 0, io.EOF
	}
	return source.reader.Read(destination)
}

func (source *ownedMaterializationSource) Close() error {
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
