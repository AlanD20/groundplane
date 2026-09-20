package agentchannel

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"io"
)

func (s *Server) sendManagedConfig(
	stream agentpb.AgentChannel_ConnectServer,
	task etcd.TaskRecord,
	assignmentID string,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (resultErr error) {
	if s.managed == nil {
		return errs.New(errs.KindInternal, "managed-config resolver is not configured")
	}
	source, err := s.managed.ResolveManagedConfig(stream.Context(), task, plan, step)
	if err != nil {
		return err
	}
	if source.Content == nil || !managedconfig.ValidMediaType(source.MediaType) || source.Length == 0 ||
		source.Length > managedconfig.MaximumArtifactBytes {
		if source.Content != nil {
			_ = source.Content.Close()
		}
		return errs.New(errs.KindInternal, "managed-config resolver returned an invalid source")
	}
	defer func() {
		if closeErr := source.Content.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	action := step.GetComponentApply()
	headerMessage := managedConfigControllerMessage(task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId())
	headerMessage.GetManagedConfigTransfer().Record = &agentpb.ManagedConfigTransfer_Header{
		Header: &agentpb.ManagedConfigTransferHeader{
			ArtifactId: action.GetArtifactId(), MediaType: source.MediaType, Length: source.Length,
			Sha256: append([]byte(nil), action.GetArtifactDigest()...),
		},
	}
	if err := stream.Send(headerMessage); err != nil {
		return err
	}
	buffer := make([]byte, managedconfig.MaximumChunkBytes)
	defer clear(buffer)
	hasher := sha256.New()
	remaining := source.Length
	var sequence uint32
	for remaining > 0 {
		readSize := min(uint64(len(buffer)), remaining)
		read, readErr := source.Content.Read(buffer[:int(readSize)])
		if read < 0 || read > int(readSize) || read == 0 && readErr == nil {
			return errs.New(errs.KindInternal, "managed-config source returned an invalid read")
		}
		if read > 0 {
			if _, err := hasher.Write(buffer[:read]); err != nil {
				return errs.New(errs.KindInternal, "managed-config source digest failed")
			}
			remaining -= uint64(read)
			sequence++
			content := append([]byte(nil), buffer[:read]...)
			message := managedConfigControllerMessage(task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId())
			message.GetManagedConfigTransfer().Record = &agentpb.ManagedConfigTransfer_Chunk{
				Chunk: &agentpb.ManagedConfigTransferChunk{Sequence: sequence, Content: content},
			}
			sendErr := stream.Send(message)
			clear(content)
			message.GetManagedConfigTransfer().GetChunk().Content = nil
			if sendErr != nil {
				return sendErr
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && remaining == 0 {
				break
			}
			return errs.New(errs.KindInternal, "managed-config source ended before its declared length")
		}
	}
	var extra [1]byte
	read, readErr := source.Content.Read(extra[:])
	clear(extra[:])
	if read != 0 || !errors.Is(readErr, io.EOF) {
		return errs.New(errs.KindInternal, "managed-config source exceeds its declared length")
	}
	digest := hasher.Sum(nil)
	defer clear(digest)
	if subtle.ConstantTimeCompare(digest, action.GetArtifactDigest()) != 1 {
		return errs.New(errs.KindInternal, "managed-config source digest does not match its plan")
	}
	endMessage := managedConfigControllerMessage(task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId())
	endMessage.GetManagedConfigTransfer().Record = &agentpb.ManagedConfigTransfer_End{
		End: &agentpb.ManagedConfigTransferEnd{ChunkCount: sequence},
	}
	return stream.Send(endMessage)
}

func managedConfigControllerMessage(
	taskID string,
	assignmentID string,
	planHash []byte,
	stepID string,
) *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ManagedConfigTransfer{
		ManagedConfigTransfer: &agentpb.ManagedConfigTransfer{
			TaskId: taskID, AssignmentId: assignmentID,
			PlanHash: append([]byte(nil), planHash...), StepId: stepID,
		},
	}}
}
