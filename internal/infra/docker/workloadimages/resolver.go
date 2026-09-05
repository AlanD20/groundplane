// Package workloadimages implements the Agent's read-only local image lookup.
package workloadimages

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

// Engine exposes inspection only: resolution cannot pull, build, or mutate.
// Its connection lifetime belongs to the Agent composition root.
type Engine interface {
	ImageInspect(context.Context, string, ...client.ImageInspectOption) (client.ImageInspectResult, error)
}

type Resolver struct{ engine Engine }

func New(engine Engine) (*Resolver, error) {
	if engine == nil {
		return nil, errs.New(errs.KindValidationFailed, "workload image engine is required")
	}
	return &Resolver{engine: engine}, nil
}

func (resolver *Resolver) Resolve(
	ctx context.Context,
	request *agentpb.ResolveWorkloadImages,
) (*agentpb.WorkloadImageResolutionResult, error) {
	if ctx == nil || resolver == nil || resolver.engine == nil {
		return nil, errs.New(errs.KindInternal, "workload image resolver is not configured")
	}
	if err := workloadimage.ValidateRequest(request); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, workloadimage.Timeout)
	defer cancel()
	result := &agentpb.WorkloadImageResolutionResult{RequestId: request.RequestId}
	success := &agentpb.WorkloadImageResolutions{}
	for index, selector := range request.Selectors {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value := selector.GetRequestedReference()
		if local := selector.GetLocalImageId(); local != "" {
			value = local
		}
		observed, err := resolver.engine.ImageInspect(ctx, value)
		if contextErr := ctx.Err(); contextErr != nil {
			return nil, contextErr
		}
		failure := agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_UNSPECIFIED
		switch {
		case errdefs.IsNotFound(err):
			failure = agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_NOT_FOUND
		case err != nil || !workloadimage.LocalIDValid(observed.ID):
			failure = agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_OBSERVATION_FAILED
		case selector.GetLocalImageId() != "" && selector.GetLocalImageId() != observed.ID:
			failure = agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_IDENTITY_MISMATCH
		}
		if failure != agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_UNSPECIFIED {
			ordinal := uint32(index)
			result.Outcome = &agentpb.WorkloadImageResolutionResult_Failure{
				Failure: &agentpb.WorkloadImageResolutionFailure{
					SelectorOrdinal: &ordinal, Kind: failure,
				},
			}
			return result, nil
		}
		success.Resolutions = append(success.Resolutions, &agentpb.WorkloadImageResolution{
			Selector: proto.CloneOf(selector), LocalImageId: observed.ID,
		})
	}
	result.Outcome = &agentpb.WorkloadImageResolutionResult_Success{Success: success}
	return result, nil
}
