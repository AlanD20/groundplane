package composehelpercontainer

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *Executor) ExecuteRecovery(
	ctx context.Context,
	request *agentpb.ReleaseRecoveryHelperRequest,
) (*agentpb.ReleaseRecoveryHelperResponse, error) {
	if executor == nil || executor.engine == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery helper container is not configured")
	}
	input, report, err := composehelper.OpenRecoveryRequest(request)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return nil, err
	}
	artifacts := input.Input.Artifacts
	if prior := artifacts.Prior; prior != nil {
		if prior.Artifact.AuthorizedVolumeDir != artifacts.Candidate.AuthorizedVolumeDir ||
			prior.Retained != nil && prior.Retained.AuthorizedVolumeDir != artifacts.Candidate.AuthorizedVolumeDir {
			return nil, errs.New(
				errs.KindStateConflict,
				"Release recovery artifacts name different Environment volume roots",
			)
		}
		var services []*agentpb.ComposeService
		if !request.InspectOnly {
			switch report.Progress.Phase {
			case releaseexecution.RestoreServingRuntime:
				services = append(services, prior.Workloads...)
				if prior.Proxy != nil {
					services = append(services, prior.Proxy)
				}
			case releaseexecution.RemoveFailedCandidate:
				for _, service := range prior.Retained.GetServices() {
					if service.ServiceId == input.Input.Release.ServiceId {
						services = append(services, service)
					}
				}
			}
		}
		for _, service := range services {
			if err := validatePreparedServiceImage(service); err != nil {
				return nil, err
			}
		}
		for _, service := range services {
			if err := executor.prepareServiceImage(bounded, service); err != nil {
				return nil, err
			}
		}
	}
	framed, err := composehelper.MarshalRecoveryRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(framed)
	options := createOptions(executor.image, "release-recovery-helper", artifacts.Candidate)
	options.Name = releaseHelperName(input.Input.Attempt)
	options.Config.Labels = releaseHelperLabels(input.Input.Attempt)
	raw, err := executor.executeFrame(bounded, options, framed)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	response, err := composehelper.ReadRecoveryResponse(bounded, bytes.NewReader(raw), input, report)
	if err != nil {
		return nil, err
	}
	proof, err := releaseexecution.DecodeRecoveryRuntimeProof(input, report, response)
	if err != nil {
		return nil, err
	}
	if proof.Inspection != request.InspectOnly {
		return nil, errs.New(errs.KindStateConflict, "Release recovery helper returned a different execution mode")
	}
	return response, nil
}
