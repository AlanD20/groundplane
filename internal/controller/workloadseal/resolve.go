// Package workloadseal resolves one complete publication before durable staging.
package workloadseal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Resolver is the authenticated Agent channel's read-only side-effect port.
type Resolver interface {
	ResolveWorkloadImages(
		context.Context,
		string,
		[]*agentpb.WorkloadImageSelector,
	) (*agentpb.WorkloadImageResolutionResult, error)
}

type Requested struct {
	Reference string
	Replicas  uint32
}

// Selection has exactly one authority: authored input or an existing full seal.
type Selection struct {
	Requested  *Requested
	Historical *domain.WorkloadSeal
}

func Resolve(
	ctx context.Context,
	resolver Resolver,
	agentID string,
	selections []Selection,
) ([]domain.WorkloadSeal, error) {
	if ctx == nil || resolver == nil || agentID == "" {
		return nil, errs.New(errs.KindInternal, "workload seal resolver is not configured")
	}
	seals := make([]domain.WorkloadSeal, len(selections))
	selectors := make([]*agentpb.WorkloadImageSelector, 0)
	ordinals := make([]int, len(selections))
	unique := make(map[string]int)
	for index, selection := range selections {
		if (selection.Requested == nil) == (selection.Historical == nil) {
			return nil, invalid()
		}
		var selector *agentpb.WorkloadImageSelector
		var key string
		if selection.Historical != nil {
			if err := domain.ValidateWorkloadSeal(*selection.Historical); err != nil {
				return nil, err
			}
			seals[index] = *selection.Historical
			selector = &agentpb.WorkloadImageSelector{
				Selector: &agentpb.WorkloadImageSelector_LocalImageId{LocalImageId: selection.Historical.LocalImageID},
			}
			key = "local:" + selection.Historical.LocalImageID
		} else {
			if selection.Requested.Replicas == 0 {
				return nil, invalid()
			}
			seals[index] = domain.WorkloadSeal{RequestedReference: selection.Requested.Reference, ReplicaCount: selection.Requested.Replicas}
			selector = &agentpb.WorkloadImageSelector{Selector: &agentpb.WorkloadImageSelector_RequestedReference{RequestedReference: selection.Requested.Reference}}
			key = "reference:" + selection.Requested.Reference
		}
		if _, err := workloadimage.SelectorValue(selector); err != nil {
			return nil, err
		}
		ordinal, exists := unique[key]
		if !exists {
			if len(selectors) == workloadimage.MaximumSelectors {
				return nil, invalid()
			}
			ordinal = len(selectors)
			unique[key] = ordinal
			selectors = append(selectors, selector)
		}
		ordinals[index] = ordinal
	}
	if len(selectors) == 0 {
		return seals, nil
	}
	result, err := resolver.ResolveWorkloadImages(ctx, agentID, selectors)
	if err != nil {
		return nil, err
	}
	// The channel owns correlation and session fencing. Recheck the returned
	// closed batch before converting any resolution into durable authority.
	if result == nil {
		return nil, invalid()
	}
	request := &agentpb.ResolveWorkloadImages{RequestId: result.RequestId, Selectors: selectors}
	if err := workloadimage.ValidateResult(request, result); err != nil {
		return nil, err
	}
	if result.GetSuccess() == nil {
		return nil, errs.New(errs.KindValidationFailed, "required workload image is unavailable on the Agent host")
	}
	for index, ordinal := range ordinals {
		seals[index].LocalImageID = result.GetSuccess().GetResolutions()[ordinal].GetLocalImageId()
		if err := domain.ValidateWorkloadSeal(seals[index]); err != nil {
			return nil, err
		}
	}
	return seals, nil
}

func invalid() error {
	return errs.New(
		errs.KindValidationFailed,
		"workload image publication selection is invalid or exceeds 64 unique images",
	)
}
