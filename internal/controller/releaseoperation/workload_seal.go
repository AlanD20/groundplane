package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	domain "github.com/AlanD20/groundplane/internal/core/release"
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
		seals, err = workloadseal.ResolveForProcess(
			ctx,
			service.images,
			selected.Identity.AgentID,
			selected.ProcessGeneration,
			selections,
		)
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
