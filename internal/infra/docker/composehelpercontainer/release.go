package composehelpercontainer

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ExecuteRelease transports the native operation into the same restricted
// helper sandbox. It does not adapt the input to Execute or ExecutionPlan.
// Any transport/helper/cleanup error leaves the effect outcome unknown.
func (executor *Executor) ExecuteRelease(ctx context.Context, request *agentpb.ReleaseHelperRequest) (*agentpb.ReleaseHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Release helper container is not configured")
	}
	input, report, err := composehelper.OpenReleaseRequest(request)
	if err != nil {
		return nil, err
	}
	startup := report.Progress.Action == releaseexecution.StartCandidate || report.Progress.Rolling != nil && report.Progress.Rolling.Phase == rollingrelease.StartCandidate
	removal := report.Progress.Action == releaseexecution.RemovePrevious || report.Progress.Action == releaseexecution.RetireSwitchedPrevious
	removal = removal || report.Progress.Rolling != nil && (report.Progress.Rolling.Phase == rollingrelease.RemovePredecessor || report.Progress.Rolling.Phase == rollingrelease.RemoveRetained)
	switching := report.Progress.Action == releaseexecution.SwitchTraffic || report.Progress.Rolling != nil && report.Progress.Rolling.Phase == rollingrelease.SwitchTraffic
	verifying := report.Progress.Action == releaseexecution.VerifyServing
	if !startup && !removal && !switching && !verifying {
		return nil, errs.New(errs.KindValidationFailed, "Release helper action is unsupported")
	}
	deadline := input.Attempt.Deadline
	if cursor := report.Progress.Rolling; cursor != nil && cursor.ReplicaDeadline.Before(deadline) {
		deadline = cursor.ReplicaDeadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	framed, err := composehelper.MarshalReleaseRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	if !request.InspectOnly && switching && input.Artifacts.Proxy != nil {
		if err := validatePreparedServiceImage(input.Artifacts.Proxy); err != nil {
			return nil, err
		}
		if err := executor.prepareServiceImage(bounded, input.Artifacts.Proxy); err != nil {
			return nil, err
		}
	}
	if !request.InspectOnly && startup {
		selected := input.Artifacts.Workloads
		if cursor := report.Progress.Rolling; cursor != nil {
			changes, err := input.Artifacts.Rolling.Changes()
			if err != nil {
				return nil, err
			}
			candidate := changes[cursor.Change-1].Candidate
			if candidate == nil {
				return nil, errs.New(errs.KindStateConflict, "rolling helper startup has no candidate")
			}
			selected = nil
			for _, service := range input.Artifacts.Workloads {
				if service.ComposeName == candidate.ComposeName {
					selected = append(selected, service)
				}
			}
		}
		if len(selected) == 0 {
			return nil, errs.New(errs.KindStateConflict, "Release startup has no selected images")
		}
		for _, service := range selected {
			if err := validatePreparedServiceImage(service); err != nil {
				return nil, err
			}
		}
		for _, service := range selected {
			if err := executor.prepareServiceImage(bounded, service); err != nil {
				return nil, err
			}
		}
	}
	options := createOptions(executor.image, input.Artifacts.Candidate)
	options.Config.Cmd = []string{"release-helper"}
	options.Name = releaseHelperName(input.Attempt)
	options.Config.Labels = releaseHelperLabels(input.Attempt)
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	response, err := composehelper.ReadReleaseResponse(context.Background(), bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	if err := releaseexecution.ValidateHelperResult(input, report.Progress, response); err != nil {
		return nil, err
	}
	if response.Inspection != request.InspectOnly {
		return nil, errs.New(errs.KindStateConflict, "Release helper returned a different execution mode")
	}
	return response, nil
}
