package releaseruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type RecoveryHelper interface {
	ExecuteRecovery(context.Context, *agentpb.ReleaseRecoveryHelperRequest) (*agentpb.ReleaseRecoveryHelperResponse, error)
}

func RunRecoveryHelper(ctx context.Context, helper RecoveryHelper, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, inspectOnly bool) (releaseexecution.RecoveryRuntimeProof, error) {
	if ctx == nil || helper == nil || report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight || input.Input.Attempt.StartedAt == nil {
		return releaseexecution.RecoveryRuntimeProof{}, errs.New(errs.KindStateConflict, "Release recovery helper has no acknowledged execution boundary")
	}
	assignment, err := releaseexecution.EncodeRecoveryAssignment(input)
	if err != nil {
		return releaseexecution.RecoveryRuntimeProof{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return releaseexecution.RecoveryRuntimeProof{}, err
	}
	pending := report
	pending.Ack = nil
	request, err := releaseexecution.EncodeRecoveryRequest(input, pending)
	if err != nil {
		return releaseexecution.RecoveryRuntimeProof{}, err
	}
	ack, err := releaseexecution.EncodeRecoveryAck(input, report)
	if err != nil {
		return releaseexecution.RecoveryRuntimeProof{}, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	response, err := helper.ExecuteRecovery(bounded, &agentpb.ReleaseRecoveryHelperRequest{Assignment: assignment,
		Start:  &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(*input.Input.Attempt.StartedAt)},
		Report: request, Acknowledgement: ack, InspectOnly: inspectOnly})
	if err != nil {
		return releaseexecution.RecoveryRuntimeProof{}, err
	}
	proof, err := releaseexecution.DecodeRecoveryRuntimeProof(input, report, response)
	if err != nil {
		return releaseexecution.RecoveryRuntimeProof{}, err
	}
	if proof.Inspection != inspectOnly {
		return releaseexecution.RecoveryRuntimeProof{}, errs.New(errs.KindStateConflict, "Release recovery helper execution mode differs")
	}
	return proof, nil
}
