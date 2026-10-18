package tasksecretpins

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
)

// PrepareOwned compares operation ownership on every preparation write, so a
// delayed worker cannot recreate unpublished pins after cancellation cleanup.
// The guards live only on this call's repository value, never shared state.
func (repository *Repository) PrepareOwned(ctx context.Context, operationID, taskID, workID string,
	pins []tasksecretpinrecord.Record, guards []Condition,
) (Prepared, error) {
	if len(guards) == 0 {
		return Prepared{}, validation("owned Secret preparation requires publication guards")
	}
	owned := Repository{store: repository.store, preparationGuards: append([]Condition(nil), guards...)}
	return owned.Prepare(ctx, operationID, taskID, workID, pins)
}
