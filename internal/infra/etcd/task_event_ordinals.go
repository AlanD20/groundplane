package etcd

import (
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Receipt numbering is independent of delivery sequence: cancellation may
// leave gaps, and trimming retains the largest ordinal in the step checkpoint.
// Read both at the same revision used by the start grant's Task/assignment CAS.
func (repository *TaskRepository) taskEventOrdinalBase(ctx context.Context,
	decision taskjournal.StepStartDecision, task TaskRecord, identity taskjournal.TaskEventIdentity, revision int64,
) (uint64, error) {
	if decision != taskjournal.StepStartAllowed {
		return 0, nil
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil {
		return 0, err
	}
	base := max(uint64(1), task.NextEventSequence-1)
	include := func(prior taskjournal.TaskEventIdentity) {
		if prior.AssignmentID == identity.AssignmentID && prior.StepID == identity.StepID &&
			prior.AgentID == identity.AgentID && prior.AgentGeneration == identity.AgentGeneration &&
			prior.Attempt == identity.Attempt {
			base = max(base, prior.Ordinal)
		}
	}
	for _, event := range snapshot.Events {
		include(event.Identity)
	}
	for _, checkpoint := range task.EventCheckpoints {
		include(checkpoint.Identity)
	}
	if base == math.MaxUint64 {
		return 0, errs.New(errs.KindInternal, "task event ordinal exhausted")
	}
	return base, nil
}
