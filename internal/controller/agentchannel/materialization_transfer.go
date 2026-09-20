package agentchannel

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"io"
)

func (s *Server) sendMaterialization(
	stream agentpb.AgentChannel_ConnectServer,
	task etcd.TaskRecord,
	assignmentID string,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (resultErr error) {
	if s.materials == nil {
		return errs.New(errs.KindInternal, "materialization resolver is not configured")
	}
	source, err := s.materials.ResolveMaterialization(stream.Context(), task, plan, step)
	if err != nil {
		return err
	}
	if source == nil {
		return errs.New(errs.KindInternal, "materialization resolver returned an empty source")
	}
	defer func() {
		if closeErr := source.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	materialization := step.GetMaterializeFile()
	header := &agentpb.MaterializationTransferHeader{
		ArtifactId:        materialization.GetArtifactId(),
		MaterializationId: materialization.GetMaterializationId(),
		EnvironmentId:     materialization.GetEnvironmentId(),
		RenderGeneration:  plan.GetRenderGeneration(),
		Destination:       materialization.GetDestination(),
		ServiceId:         materialization.GetServiceId(),
		ServiceName:       materialization.GetServiceName(),
		OutputKind:        materialization.GetOutputKind(),
		Uid:               materialization.GetUid(),
		Gid:               materialization.GetGid(),
		Mode:              materialization.GetMode(),
		Length:            materialization.GetLength(),
		Sha256:            append([]byte(nil), materialization.GetSha256()...),
	}
	headerMessage := materializationControllerMessage(
		task.ID,
		assignmentID,
		plan.GetPlanHash(),
		step.GetStepId(),
	)
	headerMessage.GetMaterializationTransfer().Record = &agentpb.MaterializationTransfer_Header{
		Header: header,
	}
	if err := stream.Send(headerMessage); err != nil {
		return err
	}
	buffer := make([]byte, entrymaterialization.MaximumChunkBytes)
	defer clear(buffer)
	hasher := sha256.New()
	remaining := materialization.GetLength()
	var sequence uint32
	for remaining > 0 {
		readSize := min(uint64(len(buffer)), remaining)
		read, readErr := source.Read(buffer[:int(readSize)])
		if read < 0 || read > int(readSize) || (read == 0 && readErr == nil) {
			return errs.New(errs.KindInternal, "materialization source returned an invalid read")
		}
		if read > 0 {
			if _, err := hasher.Write(buffer[:read]); err != nil {
				return errs.New(errs.KindInternal, "materialization source digest failed")
			}
			remaining -= uint64(read)
			sequence++
			content := append([]byte(nil), buffer[:read]...)
			chunk := &agentpb.MaterializationTransferChunk{Sequence: sequence, Content: content}
			chunkMessage := materializationControllerMessage(
				task.ID, assignmentID, plan.GetPlanHash(), step.GetStepId(),
			)
			chunkMessage.GetMaterializationTransfer().Record = &agentpb.MaterializationTransfer_Chunk{
				Chunk: chunk,
			}
			sendErr := stream.Send(chunkMessage)
			clear(content)
			chunk.Content = nil
			if sendErr != nil {
				return sendErr
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) && remaining == 0 {
				break
			}
			return errs.New(
				errs.KindInternal,
				"materialization source ended before its declared length",
			)
		}
	}
	var extra [1]byte
	read, readErr := source.Read(extra[:])
	clear(extra[:])
	if read != 0 || !errors.Is(readErr, io.EOF) {
		return errs.New(errs.KindInternal, "materialization source exceeds its declared length")
	}
	digest := hasher.Sum(nil)
	defer clear(digest)
	if subtle.ConstantTimeCompare(digest, materialization.GetSha256()) != 1 {
		return errs.New(errs.KindInternal, "materialization source digest does not match its plan")
	}
	endMessage := materializationControllerMessage(
		task.ID,
		assignmentID,
		plan.GetPlanHash(),
		step.GetStepId(),
	)
	endMessage.GetMaterializationTransfer().Record = &agentpb.MaterializationTransfer_End{
		End: &agentpb.MaterializationTransferEnd{ChunkCount: sequence},
	}
	return stream.Send(endMessage)
}

func materializationControllerMessage(
	taskID string,
	assignmentID string,
	planHash []byte,
	stepID string,
) *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_MaterializationTransfer{
		MaterializationTransfer: &agentpb.MaterializationTransfer{
			TaskId: taskID, AssignmentId: assignmentID,
			PlanHash: append([]byte(nil), planHash...), StepId: stepID,
		},
	}}
}
