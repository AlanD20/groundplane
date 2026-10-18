package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const ReleaseHelperArgument = "release-helper"

func RunReleaseHelper(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	request, err := composehelper.ReadReleaseRequest(ctx, stdin)
	if err != nil {
		return err
	}
	input, report, err := composehelper.OpenReleaseRequest(request)
	if err != nil {
		return err
	}
	response := &agentpb.ReleaseHelperResponse{}
	switch {
	case request.InspectOnly:
		response, err = composehelper.InspectReleaseEffect(ctx, runner.New(nil), input, report)
		if err != nil {
			return err
		}
	case report.Progress.Rolling != nil && (report.Progress.Rolling.Phase == rollingrelease.RemovePredecessor || report.Progress.Rolling.Phase == rollingrelease.RemoveRetained):
		result, err := composehelper.ExecuteReleaseReplicaRemoval(ctx, runner.New(nil), input, report)
		if err != nil {
			return err
		}
		response.Succeeded, response.MutationAttempted, response.AbsenceObserved = true, result.MutationAttempted, result.AbsenceObserved
	case report.Progress.Action == releaseexecution.VerifyServing:
		response, err = composehelper.VerifyReleaseServing(ctx, runner.New(nil), input, report)
		if err != nil {
			return err
		}
	case report.Progress.Action == releaseexecution.SwitchTraffic || report.Progress.Rolling != nil && report.Progress.Rolling.Phase == rollingrelease.SwitchTraffic:
		response, err = composehelper.ExecuteReleaseTraffic(ctx, runner.New(nil), input, report)
		if err != nil {
			return err
		}
	case report.Progress.Action == releaseexecution.StartCandidate || report.Progress.Rolling != nil && report.Progress.Rolling.Phase == rollingrelease.StartCandidate:
		result, err := composehelper.ExecuteReleaseStartup(ctx, runner.New(nil), input, report)
		if err != nil {
			return err
		}
		response.Succeeded, response.MutationAttempted, response.ExitCode = true, result.MutationAttempted, int32(
			result.ExitCode,
		)
	case report.Progress.Action == releaseexecution.RemovePrevious || report.Progress.Action == releaseexecution.RetireSwitchedPrevious:
		result, err := composehelper.ExecuteReleasePreviousRemoval(ctx, runner.New(nil), input, report)
		if err != nil {
			return err
		}
		response.Succeeded, response.MutationAttempted, response.AbsenceObserved = true, result.MutationAttempted, result.AbsenceObserved
	default:
		return errs.New(
			errs.KindValidationFailed,
			"Release helper request does not select an implemented host mutation",
		)
	}
	return composehelper.WriteReleaseResponse(ctx, stdout, response)
}
