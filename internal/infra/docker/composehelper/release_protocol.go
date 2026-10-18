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

// OpenReleaseRequest validates a local, trusted-parent helper envelope. Network
// authentication remains the Agent channel's responsibility; these receipts are
// bound to each other, not treated as independently signed credentials.
func OpenReleaseRequest(request *agentpb.ReleaseHelperRequest) (releaseexecution.Input, releaseexecution.CheckpointReport, error) {
	empty, noReport := releaseexecution.Input{}, releaseexecution.CheckpointReport{}
	if request == nil || executionplan.RejectUnknown(request) != nil || request.Assignment == nil || request.Assignment.Attempt == nil {
		return empty, noReport, invalidReleaseHelper()
	}
	identity := request.Assignment.Attempt
	attempt, release, err := workexecution.DecodeReleaseDelivery(request.Assignment, workexecution.Identity{
		Executor: workexecution.Agent, HostID: identity.HostId, AgentID: identity.AgentId, Generation: identity.ExecutorGeneration})
	if err != nil {
		return empty, noReport, err
	}
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(attempt, release)
	if err != nil {
		return empty, noReport, err
	}
	input, _, err := (releaseexecution.Input{Attempt: attempt, Release: release, Artifacts: artifacts}).WithStart(request.Start)
	if err != nil {
		return empty, noReport, err
	}
	report, err := releaseexecution.DecodeCheckpointRequest(input, request.Report)
	if err != nil {
		return empty, noReport, err
	}
	ack, err := releaseexecution.DecodeCheckpointAck(input, report, request.Acknowledgement)
	if err != nil {
		return empty, noReport, err
	}
	report.Ack = &ack
	if !report.Progress.InFlight || report.Progress.Phase != releaseexecution.Rollout {
		return empty, noReport, invalidReleaseHelper()
	}
	return input, report, nil
}

func MarshalReleaseRequest(request *agentpb.ReleaseHelperRequest) ([]byte, error) {
	if _, _, err := OpenReleaseRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadReleaseRequest(ctx context.Context, input io.Reader) (*agentpb.ReleaseHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	request := &agentpb.ReleaseHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, invalidReleaseHelper()
	}
	if _, _, err := OpenReleaseRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func WriteReleaseResponse(ctx context.Context, output io.Writer, response *agentpb.ReleaseHelperResponse) error {
	if ctx == nil || output == nil {
		return invalidReleaseHelper()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateReleaseResponse(response); err != nil {
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
	for len(framed) > 0 {
		n, err := output.Write(framed)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if n <= 0 {
			return errs.New(errs.KindInternal, "Release helper response writer made no progress")
		}
		framed = framed[n:]
	}
	return nil
}

func ReadReleaseResponse(ctx context.Context, input io.Reader) (*agentpb.ReleaseHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	response := &agentpb.ReleaseHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, invalidReleaseHelper()
	}
	return response, validateReleaseResponse(response)
}

func validateReleaseResponse(response *agentpb.ReleaseHelperResponse) error {
	if response == nil || executionplan.RejectUnknown(response) != nil || response.ExitCode < 0 ||
		response.Succeeded && response.ExitCode != 0 || response.AbsenceObserved && !response.Succeeded || response.Inspection && response.MutationAttempted {
		return invalidReleaseHelper()
	}
	return nil
}

func invalidReleaseHelper() error {
	return errs.New(errs.KindValidationFailed, "native Release helper envelope is invalid")
}
