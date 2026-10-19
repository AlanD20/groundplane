package scriptsourcereference

import "context"

// PrepareOwned fences every preparation write with its owner's captured state.
// A delayed preparer cannot recreate references after stopped work is cleaned up.
func (repository *Repository) PrepareOwned(
	ctx context.Context,
	operationID string,
	members []Member,
	guards []Condition,
) (Prepared, error) {
	if len(guards) == 0 {
		return Prepared{}, validation("owned source preparation requires publication guards")
	}
	for _, guard := range guards {
		if guard.Key == "" || guard.Prefix || guard.ModRevision <= 0 {
			return Prepared{}, validation("source preparation owner guards require exact existing records")
		}
	}
	owned := Repository{
		store:             repository.store,
		script:            repository.script,
		preparationGuards: append([]Condition(nil), guards...),
	}
	return owned.Prepare(ctx, operationID, members)
}

func (repository *Repository) guardPreparation(conditions []Condition) []Condition {
	return append(conditions, repository.preparationGuards...)
}
