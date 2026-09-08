package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
)

func (service *Service) sealCandidates(ctx context.Context, candidates []releaseCandidateInput) error {
	selections := make([]workloadseal.Selection, len(candidates))
	for index, candidate := range candidates {
		selections[index] = candidate.selection
	}
	for _, candidate := range candidates {
		if candidate.priorWorkload != nil {
			selections = append(selections, workloadseal.Selection{Historical: candidate.priorWorkload})
		}
	}
	agent, err := service.agents.GetSingleton(ctx)
	if err != nil {
		return err
	}
	seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID, selections)
	if err != nil {
		return err
	}
	for index := range candidates {
		candidates[index].workload = seals[index]
	}
	return nil
}
