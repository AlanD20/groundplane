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
	requirementGate := etcd.BlueprintRequirementGate{}
	if len(requirements.Resolved) != 0 {
		inherited, gateErr := environmentBlueprintRequirementTaskEdges(
			ctx, service.repository, task.ID, requirements, requirements.ResolutionRevision,
		)
		if gateErr != nil {
			return etcd.BlueprintRequirementGate{}, gateErr
		}
		stepIDs := make([]string, len(task.Steps))
		for index, step := range task.Steps {
			stepIDs[index] = step.ID
		}
		dag, gateErr := core.BuildBlueprintRequirementDAG(task.ID, requirements, stepIDs, inherited)
		if gateErr != nil {
			return etcd.BlueprintRequirementGate{}, gateErr
		}
		requirementGate, gateErr = etcd.NewBlueprintRequirementGate(
			task, requirements.ResolutionRevision, dag,
		)
		if gateErr != nil {
			return etcd.BlueprintRequirementGate{}, gateErr
		}
		task.Params[etcd.TaskBlueprintRequirementGateSHA256Param] = requirementGate.DAGDigest
	}
	return requirementGate, nil
}
