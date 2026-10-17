package volumeruntime

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (executor *Executor) runRecovery(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	saved, err := executor.journal.PrepareVolumeRecovery(ctx, input.attempt, agentworkjournal.VolumeRecoveryRecord{
		Action: input.procedure.Action, VolumeID: input.procedure.VolumeId, Source: *input.recovery, Boundary: input.boundary,
	})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved.Receipt != nil {
		return *saved.Receipt, nil
	}
	if saved.Failure != nil {
		return executor.deliverFailure(ctx, input, exchange, saved.Failure)
	}
	if saved.Report == nil {
		inspectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		observation, err := executor.host.InspectRecovery(inspectCtx, input)
		cancel()
		if err != nil {
			return executor.fail(ctx, input, exchange, err)
		}
		report := volumeexecution.RecoveryReport{AttemptID: input.attempt.ID, ForwardID: input.recovery.Forward.ID,
			Failure: input.recovery.Evidence, HelperStopped: true, Observation: observation}
		if err := executor.journal.SaveVolumeRecoveryReport(ctx, input.attempt, report); err != nil {
			return workexecution.Attempt{}, err
		}
		saved.Report = &report
	}
	raw, evidence, err := volumeexecution.EncodeRecoveryReport(input.attempt, *input.recovery, input.procedure.Action, input.procedure.VolumeId, input.boundary, *saved.Report)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.RecoverVolume(ctx, &agentpb.VolumeWorkRecoveryReportRequest{Attempt: identity, Report: raw})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if ack == nil || executionplan.RejectUnknown(ack) != nil || ack.Evidence == nil || ack.FinishedAt == nil || ack.FinishedAt.CheckValid() != nil ||
		ack.FinishedAt.AsTime().Before(saved.Report.Observation.At) || ack.BoundaryCompleted != saved.Report.BoundaryCompleted(input.boundary) {
		return workexecution.Attempt{}, invalidInput()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, input.attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, input.attempt) || ack.Evidence.InputId != evidence.ID || hex.EncodeToString(ack.Evidence.Sha256) != evidence.SHA256 {
		return workexecution.Attempt{}, invalidInput()
	}
	terminal := input.attempt
	finished := ack.FinishedAt.AsTime()
	terminal.FinishedAt, terminal.Result, terminal.Outcome = &finished, &evidence, workexecution.Failed
	if ack.BoundaryCompleted {
		terminal.Outcome = workexecution.Succeeded
	}
	if err := executor.journal.AcknowledgeVolumeRecovery(ctx, input.attempt, terminal); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (host *RuntimeHost) InspectRecovery(ctx context.Context, input Input) (volumeexecution.Observation, error) {
	if input.recovery == nil || input.procedure == nil || input.attempt.StartedAt == nil {
		return volumeexecution.Observation{}, invalidInput()
	}
	if err := host.ownership.Check(); err != nil {
		return volumeexecution.Observation{}, err
	}
	source, err := volumeexecution.EncodeRecovery(*input.recovery)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	boundary, err := volumeexecution.EncodeProgress(input.recovery.Forward, input.procedure.Action, input.procedure.VolumeId, input.boundary)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	request := &agentpb.VolumeWorkHelperRequest{
		Assignment: &agentpb.VolumeWorkAssignment{Attempt: identity, Input: input.procedure, RecoverySource: source, RecoveryBoundary: boundary},
		Start:      &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*input.attempt.StartedAt)}, InspectOnly: true,
	}
	captured, err := executionplan.OpenVolumeHelperRequest(request)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	response, err := host.helper.ExecuteVolumeWork(ctx, host.root, request)
	if err != nil {
		return volumeexecution.Observation{}, err
	}
	if err := host.ownership.Check(); err != nil {
		return volumeexecution.Observation{}, err
	}
	return executionplan.ReadVolumeHelperObservation(captured, response)
}
