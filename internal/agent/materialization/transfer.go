package materialization

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

func (inbox *Inbox) Accept(
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

func (inbox *Inbox) acceptHeader(
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

func (inbox *Inbox) acceptChunk(
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

func (inbox *Inbox) acceptEnd(
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
	if step.reusable {
		for _, sibling := range task.steps {
			if sibling == step || sibling.group != step.group || sibling.shared == nil {
				continue
			}
			if !bytes.Equal(sibling.shared.content, step.content) {
				return inbox.failStep(step, "agent: recovery materialization pair content differs")
			}
			clear(step.content)
			step.content = nil
			step.shared = sibling.shared
			step.shared.refs++
			break
		}
		if step.shared == nil {
			step.shared = &sharedMaterializationContent{content: step.content, refs: 1}
			step.content = nil
		}
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

func (inbox *Inbox) failStep(step *materializationStepInbox, message string) error {
	err := errs.New(errs.KindInternal, message)
	if step.state != materializationFailed && step.state != materializationConsumed {
		inbox.destroyStep(step)
		step.err = err
		if step.state != materializationComplete {
			select {
			case <-step.ready:
			default:
				close(step.ready)
			}
		}
		step.state = materializationFailed
	}
	return err
}
