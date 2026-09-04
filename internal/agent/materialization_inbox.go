package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"sync"
	"time"

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
	assignmentID     string
	planHash         [sha256.Size]byte
	renderGeneration uint64
	deadline         time.Time
	steps            map[string]*materializationStepInbox
	retired          bool
	expiry           *time.Timer
}

type materializationStepInbox struct {
	expected *agentpb.MaterializeFile
	header   entrymaterialization.Header
	content  []byte
	chunks   uint32
	received uint64
	digest   entrymaterialization.Hasher
	state    materializationTransferState
	retired  bool
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
	maximumMaterializationTransferChunks uint32 = 32
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
		assignmentID: assignment.AssignmentID,
		planHash: hashForPlan(
			assignment.Plan,
		),
		renderGeneration: assignment.Plan.GetRenderGeneration(),
		deadline:         assignment.Deadline,
		steps:            steps,
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if existing := inbox.tasks[assignment.TaskID]; existing != nil {
		if !existing.retired || existing.assignmentID != task.assignmentID ||
			existing.planHash != task.planHash || existing.renderGeneration != task.renderGeneration ||
			!existing.deadline.Equal(task.deadline) {
			return errs.New(errs.KindInternal, "agent: materialization task was registered twice")
		}
		inbox.destroyTask(existing)
	}
	inbox.tasks[assignment.TaskID] = task
	return nil
}

func (inbox *materializationInbox) Accept(
	ctx context.Context,
	transfer *agentpb.MaterializationTransfer,
) error {
	if transfer != nil {
		if chunk := transfer.GetChunk(); chunk != nil {
			defer func() {
				clear(chunk.Content)
				chunk.Content = nil
			}()
		}
	}
	if ctx == nil || inbox == nil || transfer == nil {
		return errs.New(errs.KindInternal, "agent: materialization transfer is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	task := inbox.tasks[transfer.GetTaskId()]
	if task != nil && !time.Now().Before(task.deadline) {
		if task.retired {
			inbox.destroyTask(task)
			delete(inbox.tasks, transfer.GetTaskId())
		}
		return errs.New(errs.KindInternal, "agent: materialization transfer correlation is expired")
	}
	if task == nil || transfer.GetAssignmentId() != task.assignmentID || len(transfer.GetPlanHash()) != sha256.Size ||
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
		return inbox.acceptEnd(transfer.GetTaskId(), transfer.GetStepId(), task, step, record.End)
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
		TaskID:        transfer.GetTaskId(),
		StepID:        transfer.GetStepId(),
		EnvironmentID: header.GetEnvironmentId(),
		Generation:    header.GetRenderGeneration(),
		Destination:   header.GetDestination(),
		ServiceID:     header.GetServiceId(),
		ServiceName:   header.GetServiceName(),
		OutputKind:    outputKind,
		UID:           header.GetUid(),
		GID:           header.GetGid(),
		Mode:          entrymaterialization.Mode(header.GetMode()),
		Length:        header.GetLength(),
		Digest:        digest,
	})
	if err != nil || header.GetRenderGeneration() != task.renderGeneration {
		return inbox.failStep(step, "agent: materialization transfer header policy is invalid")
	}
	step.header = validated
	if step.retired {
		if step.digest != nil {
			step.digest.Destroy()
		}
		step.digest = entrymaterialization.NewHasher()
	} else {
		step.content = make([]byte, 0, int(header.GetLength()))
	}
	step.state = materializationReceiving
	return nil
}

func (inbox *materializationInbox) acceptChunk(
	step *materializationStepInbox,
	chunk *agentpb.MaterializationTransferChunk,
) error {
	if step.state != materializationReceiving || chunk == nil || chunk.GetSequence() != step.chunks+1 ||
		len(chunk.GetContent()) == 0 || len(chunk.GetContent()) > entrymaterialization.MaximumChunkBytes ||
		step.chunks >= maximumMaterializationTransferChunks ||
		step.received+uint64(len(chunk.GetContent())) > step.expected.GetLength() {
		return inbox.failStep(step, "agent: materialization transfer chunk is invalid")
	}
	if step.retired {
		if step.digest == nil {
			return inbox.failStep(step, "agent: materialization transfer digest state is invalid")
		}
		written, err := step.digest.Write(chunk.GetContent())
		if err != nil || written != len(chunk.GetContent()) {
			return inbox.failStep(step, "agent: materialization transfer digest state is invalid")
		}
	} else {
		step.content = append(step.content, chunk.GetContent()...)
	}
	step.received += uint64(len(chunk.GetContent()))
	step.chunks++
	return nil
}

func (inbox *materializationInbox) acceptEnd(
	taskID string,
	stepID string,
	task *materializationTaskInbox,
	step *materializationStepInbox,
	end *agentpb.MaterializationTransferEnd,
) error {
	if step.state != materializationReceiving || end == nil || end.GetChunkCount() != step.chunks ||
		step.received != step.expected.GetLength() {
		return inbox.failStep(step, "agent: materialization transfer end is invalid")
	}
	if step.retired {
		if step.digest == nil {
			return inbox.failStep(step, "agent: materialization transfer digest state is invalid")
		}
		var expected entrymaterialization.Digest
		copy(expected[:], step.expected.GetSha256())
		digest := step.digest
		step.digest = nil
		if !digest.Verify(expected) {
			return inbox.failStep(step, "agent: materialization transfer digest is invalid")
		}
		delete(task.steps, stepID)
		if len(task.steps) == 0 {
			inbox.destroyTask(task)
			delete(inbox.tasks, taskID)
		}
		return nil
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
		header.GetServiceName() == expected.GetServiceName() &&
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
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_GENERATED_ENV:
		return entrymaterialization.OutputRemoveGeneratedEnv, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_PLAIN_FILE:
		return entrymaterialization.OutputRemovePlainFile, nil
	case agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_REMOVE_SECRET_FILE:
		return entrymaterialization.OutputRemoveSecretFile, nil
	default:
		return 0, errs.New(errs.KindInternal, "agent: materialization output kind is invalid")
	}
}

func (inbox *materializationInbox) failStep(step *materializationStepInbox, message string) error {
	err := errs.New(errs.KindInternal, message)
	if step.state != materializationFailed && step.state != materializationConsumed {
		inbox.destroyStep(step)
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
		inbox.Retire(taskID)
		return materializationPayload{}, ctx.Err()
	case <-ready:
	}
	inbox.mu.Lock()
	defer inbox.mu.Unlock()
	if step.retired {
		if err := ctx.Err(); err != nil {
			return materializationPayload{}, err
		}
		return materializationPayload{}, errs.New(errs.KindInternal, "agent: materialization payload was retired")
	}
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

// Retire clears all buffered plaintext after worker terminalization while
// retaining enough exact authority to validate transfers already in transport.
func (inbox *materializationInbox) Retire(taskID string) {
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

func (inbox *materializationInbox) scheduleExpiry(taskID string, task *materializationTaskInbox) {
	assignmentID := task.assignmentID
	planHash := task.planHash
	deadline := task.deadline
	task.expiry = time.AfterFunc(time.Until(deadline), func() {
		inbox.expire(taskID, task, assignmentID, planHash, deadline)
	})
}

func (inbox *materializationInbox) expire(
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

func (inbox *materializationInbox) destroyStep(step *materializationStepInbox) {
	clear(step.content)
	step.content = nil
	if step.digest != nil {
		step.digest.Destroy()
		step.digest = nil
	}
}

func (inbox *materializationInbox) destroyTask(task *materializationTaskInbox) {
	if task.expiry != nil {
		task.expiry.Stop()
		task.expiry = nil
	}
	for _, step := range task.steps {
		inbox.destroyStep(step)
	}
}

// Release is a hard rollback for assignments that were never admitted.
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
			inbox.destroyStep(step)
		}
	}
	inbox.destroyTask(task)
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
