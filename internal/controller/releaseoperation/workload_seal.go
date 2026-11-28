package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) sealCandidates(ctx context.Context, candidates []releaseCandidateInput) error {
	return service.sealCandidatesOnHost(ctx, candidates, nil)
}

func (service *Service) sealCandidatesOnHost(ctx context.Context, candidates []releaseCandidateInput,
	selected *hostplacement.Snapshot,
) error {
	selections := make([]workloadseal.Selection, len(candidates))
	for index, candidate := range candidates {
		selections[index] = candidate.selection
	}
	for _, candidate := range candidates {
		if candidate.priorWorkload != nil {
			selections = append(selections, workloadseal.Selection{Historical: candidate.priorWorkload})
		}
	}
	var seals []domain.WorkloadSeal
	var err error
	if selected != nil {
		if len(candidates) != 1 || candidates[0].remoteProxyImage == nil {
			return remotePlacementUnavailable()
		}
		proxy := *candidates[0].remoteProxyImage
		selections = append(selections, workloadseal.Selection{
			Requested: &workloadseal.Requested{Reference: proxy.Reference(), Replicas: 1},
		})
		seals, err = workloadseal.ResolveForProcess(
			ctx,
			service.images,
			selected.Identity.AgentID,
			selected.ProcessGeneration,
			selections,
		)
		if err == nil && seals[len(seals)-1].LocalImageID != "sha256:"+proxy.Platform.ConfigDigest {
			return errs.New(errs.KindStateConflict, "selected Host proxy image differs from the attested image")
		}
	} else {
		agent, readErr := service.agents.GetSingleton(ctx)
		if readErr != nil {
			return readErr
		}
		seals, err = workloadseal.Resolve(ctx, service.images, agent.Record.ID, selections)
	}
	if err != nil {
		return err
	}
	for index := range candidates {
		candidates[index].workload = seals[index]
	}
	return nil
}
