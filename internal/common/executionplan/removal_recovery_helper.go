package executionplan

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type RemovalRecoveryHelperInput struct {
	Attempt     workexecution.Attempt
	Procedure   *agentpb.ServiceLifecycleWorkInput
	Capture     removalexecution.RecoveryCapture
	Progress    removalexecution.Progress
	Checkpoint  workexecution.Checkpoint
	InspectOnly bool
}

func OpenRemovalRecoveryHelperRequest(
	request *agentpb.ServiceRemovalRecoveryHelperRequest,
) (RemovalRecoveryHelperInput, error) {
	if request == nil || proto.Size(request) > MaximumPlanBytes || RejectUnknown(request) != nil ||
		request.Assignment == nil || request.Assignment.Attempt == nil || request.Report == nil || request.Acknowledgement == nil {
		return RemovalRecoveryHelperInput{}, invalidRemovalHelper()
	}
	a := request.Assignment
	identity := workexecution.Identity{
		Executor:   workexecution.Agent,
		HostID:     a.Attempt.HostId,
		AgentID:    a.Attempt.AgentId,
		Generation: a.Attempt.ExecutorGeneration,
	}
	attempt, err := workexecution.DecodeAgentAttempt(a.Attempt, identity)
	if err != nil {
		return RemovalRecoveryHelperInput{}, err
	}
	if ValidateServiceLifecycleWorkInput(a.Removal) != nil || attempt.Direction != workexecution.Recovery ||
		a.Removal.CapturedSource.InputId != attempt.Input.ID || hex.EncodeToString(a.Removal.CapturedSource.Sha256) != attempt.Input.SHA256 {
		return RemovalRecoveryHelperInput{}, invalidRemovalHelper()
	}
	if ref := a.Configuration; ref != nil && (ids.Validate(ids.KindConfig, ref.ConfigurationId) != nil ||
		ref.EnvironmentId != a.Removal.EnvironmentId || ref.Generation == 0 || len(ref.Sha256) != sha256.Size) {
		return RemovalRecoveryHelperInput{}, invalidRemovalHelper()
	}
	capture, err := removalexecution.DecodeCapture(a.Removal, a.CapturedFailure)
	if err != nil {
		return RemovalRecoveryHelperInput{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(request.Start, attempt)
	if err != nil {
		return RemovalRecoveryHelperInput{}, err
	}
	attempt.StartedAt = &started
	reported, err := workexecution.DecodeAgentAttempt(request.Report.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(reported, attempt) {
		return RemovalRecoveryHelperInput{}, invalidRemovalHelper()
	}
	progress, err := removalexecution.DecodeProgress(attempt, a.Removal, capture, request.Report.Progress)
	if err != nil || !progress.InFlight {
		return RemovalRecoveryHelperInput{}, invalidRemovalHelper()
	}
	checkpoint, err := progress.Checkpoint(attempt, a.Removal, capture)
	if err != nil {
		return RemovalRecoveryHelperInput{}, err
	}
	ack := request.Acknowledgement
	acknowledged, err := workexecution.DecodeAgentAttempt(ack.Attempt, identity)
	if err != nil || !workexecution.SameAssignment(acknowledged, attempt) || ack.Evidence == nil ||
		ack.RecordedAt == nil || ack.RecordedAt.CheckValid() != nil {
		return RemovalRecoveryHelperInput{}, invalidRemovalHelper()
	}
	if checkpoint.Sequence != ack.Sequence || checkpoint.StageID != ack.StageId ||
		!checkpoint.RecordedAt.Equal(ack.RecordedAt.AsTime()) ||
		checkpoint.Evidence.ID != ack.Evidence.InputId ||
		checkpoint.Evidence.SHA256 != hex.EncodeToString(ack.Evidence.Sha256) {
		return RemovalRecoveryHelperInput{}, invalidRemovalHelper()
	}
	return RemovalRecoveryHelperInput{
		Attempt:     attempt,
		Procedure:   a.Removal,
		Capture:     capture,
		Progress:    progress,
		Checkpoint:  checkpoint,
		InspectOnly: request.InspectOnly,
	}, nil
}

func ReadRemovalRecoveryHelperReceipt(
	input RemovalRecoveryHelperInput,
	response *agentpb.ServiceRemovalRecoveryHelperResponse,
) (*workexecution.Input, error) {
	if input.Progress.Validate(input.Attempt, input.Procedure, input.Capture) != nil || !input.Progress.InFlight ||
		response == nil || RejectUnknown(response) != nil || response.StageId != input.Checkpoint.StageID ||
		response.Inspection != input.InspectOnly || response.ObservedAt == nil || response.ObservedAt.CheckValid() != nil ||
		response.ObservedAt.AsTime().Before(input.Progress.UpdatedAt) {
		return nil, invalidRemovalHelper()
	}
	if !response.Satisfied {
		if !input.InspectOnly || response.Evidence != nil {
			return nil, invalidRemovalHelper()
		}
		return nil, nil
	}
	if response.Evidence == nil {
		return nil, invalidRemovalHelper()
	}
	evidence := workexecution.Input{ID: response.Evidence.InputId, SHA256: hex.EncodeToString(response.Evidence.Sha256)}
	if evidence.Validate() != nil || evidence.ID != input.Attempt.Input.ID {
		return nil, invalidRemovalHelper()
	}
	return &evidence, nil
}

func invalidRemovalHelper() error {
	return errs.New(
		errs.KindValidationFailed,
		"Service removal restoration helper lacks its exact acknowledged recovery grant",
	)
}
