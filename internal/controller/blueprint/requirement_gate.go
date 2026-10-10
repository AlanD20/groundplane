package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func (service *Service) prepareRequirementGate(
	ctx context.Context,
	task etcd.TaskRecord,
	requirements core.BlueprintRequirements,
) (etcd.BlueprintRequirementGate, error) {
	if len(requirements.Resolved) != 0 {
		inherited, gateErr := environmentBlueprintRequirementTaskEdges(
			ctx, service.repository, task.ID, requirements, requirements.ResolutionRevision,
		)
		if gateErr != nil {
			return etcd.BlueprintRequirementGate{}, gateErr
		}
		return prepareRequirementGateWithEdges(task, requirements, inherited)
	}
	return prepareRequirementGateWithEdges(task, requirements, nil)
}

func prepareRequirementGateWithEdges(
	task etcd.TaskRecord,
	requirements core.BlueprintRequirements,
	inherited []core.BlueprintRequirementTaskEdge,
) (etcd.BlueprintRequirementGate, error) {
	if len(requirements.Resolved) == 0 {
		return etcd.BlueprintRequirementGate{}, nil
	}
	stepIDs := make([]string, len(task.Steps))
	for index, step := range task.Steps {
		stepIDs[index] = step.ID
	}
	dag, err := core.BuildBlueprintRequirementDAG(task.ID, requirements, stepIDs, inherited)
	if err != nil {
		return etcd.BlueprintRequirementGate{}, err
	}
	requirementGate, err := etcd.NewBlueprintRequirementGate(
		task, requirements.ResolutionRevision, dag,
	)
	if err != nil {
		return etcd.BlueprintRequirementGate{}, err
	}
	task.Params[etcd.TaskBlueprintRequirementGateSHA256Param] = requirementGate.DAGDigest
	return requirementGate, nil
}
