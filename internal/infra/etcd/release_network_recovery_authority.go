package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) releaseNetworkRecoveryProcedureAtRevision(ctx context.Context,
	task TaskRecord, revision int64,
) (*agentpb.ManagedNetworkRecoveryProcedure, []keyvalue.Condition, bool, error) {
	if !taskjournal.IsReleaseNetworkChild(task.Params) {
		return nil, nil, false, nil
	}
	dependency, applies, err := repository.readReleaseNetworkDependency(ctx, task, revision)
	if err != nil {
		return nil, nil, true, err
	}
	if !applies || dependency.child.Record.ID != task.ID {
		return nil, nil, true, errs.New(
			errs.KindStateConflict,
			"network preparation recovery lost its release authority",
		)
	}
	plan, err := executionplan.BuildReleaseNetworkPreparation(
		task.PlanID,
		uint64(task.RenderGeneration),
		uint32(task.TimeoutSeconds),
		dependency.preparation,
	)
	if err != nil {
		return nil, nil, true, err
	}
	if hex.EncodeToString(plan.GetPlanHash()) != task.PlanHash || len(task.Steps) != len(plan.GetSteps()) {
		return nil, nil, true, errs.New(
			errs.KindStateConflict,
			"network preparation recovery differs from its captured plan",
		)
	}
	for index, step := range plan.GetSteps() {
		if task.Steps[index].ID != step.GetStepId() {
			return nil, nil, true, errs.New(errs.KindStateConflict, "network preparation recovery step changed")
		}
	}
	return plan.GetManagedNetworkRecoveryProcedure(), dependency.conditions, true, nil
}
