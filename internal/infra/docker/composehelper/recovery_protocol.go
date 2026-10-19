package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func OpenRecoveryRequest(
	request *agentpb.ReleaseRecoveryHelperRequest,
) (releaseexecution.RecoveryInput, releaseexecution.RecoveryReport, error) {
	empty, noReport := releaseexecution.RecoveryInput{}, releaseexecution.RecoveryReport{}
	if request == nil || executionplan.RejectUnknown(request) != nil ||
		request.Assignment.GetWork().GetAttempt() == nil {
		return empty, noReport, invalidReleaseHelper()
	}
	identity := request.Assignment.Work.Attempt
	input, err := releaseexecution.DecodeRecoveryAssignment(request.Assignment, workexecution.Identity{
		Executor: workexecution.Agent, HostID: identity.HostId, AgentID: identity.AgentId, Generation: identity.ExecutorGeneration})
	if err != nil {
		return empty, noReport, err
	}
	input.Input, _, err = input.Input.WithStart(request.Start)
	if err != nil {
		return empty, noReport, err
	}
	report, err := releaseexecution.DecodeRecoveryRequest(input, request.Report)
	if err != nil {
		return empty, noReport, err
	}
	ack, err := releaseexecution.DecodeRecoveryAck(input, report, request.Acknowledgement)
	if err != nil {
		return empty, noReport, err
	}
	report.Ack = &ack
	if !report.Progress.InFlight {
		return empty, noReport, invalidReleaseHelper()
	}
	if input.PreservesServing() && !request.InspectOnly {
		return empty, noReport, invalidReleaseHelper()
	}
	switch report.Progress.Phase {
	case releaseexecution.InspectFailedRuntime, releaseexecution.RestoreServingRuntime,
		releaseexecution.RemoveFailedCandidate, releaseexecution.VerifyRestoration:
		return input, report, nil
	default:
		return empty, noReport, invalidReleaseHelper()
	}
}

func MarshalRecoveryRequest(request *agentpb.ReleaseRecoveryHelperRequest) ([]byte, error) {
	if _, _, err := OpenRecoveryRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadRecoveryRequest(ctx context.Context, input io.Reader) (*agentpb.ReleaseRecoveryHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.ReleaseRecoveryHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, invalidReleaseHelper()
	}
	if _, _, err := OpenRecoveryRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func WriteRecoveryResponse(
	ctx context.Context,
	output io.Writer,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
	response *agentpb.ReleaseRecoveryHelperResponse,
) error {
	if ctx == nil || output == nil {
		return invalidReleaseHelper()
	}
	if _, err := releaseexecution.DecodeRecoveryRuntimeProof(input, report, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	framed, err := frame(raw)
	if err != nil {
		return err
	}
	for len(framed) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := output.Write(framed)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if n <= 0 {
			return errs.New(errs.KindInternal, "Recovery helper response writer made no progress")
		}
		framed = framed[n:]
	}
	return nil
}

func ReadRecoveryResponse(
	ctx context.Context,
	reader io.Reader,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (*agentpb.ReleaseRecoveryHelperResponse, error) {
	raw, err := readFrame(ctx, reader)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	response := &agentpb.ReleaseRecoveryHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, invalidReleaseHelper()
	}
	if _, err := releaseexecution.DecodeRecoveryRuntimeProof(input, report, response); err != nil {
		return nil, err
	}
	return response, nil
}
