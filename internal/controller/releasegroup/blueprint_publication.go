package releasegroup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
)

// PrepareNext reconciles at most one definition. The collection epoch fences
// the complete snapshot; persisted definitions are the restart receipt. No
// execution work is opened until every definition matches the accepted input.
func (planner *ReleaseGroupBlueprintPlanner) PrepareNext(
	ctx context.Context, environmentID string, specs map[string]core.ReleaseGroupSpec,
	services []core.Service, allocate ReleaseGroupIDAllocator,
) (groupstore.ReleaseGroupBlueprintPreparedMutation, bool, error) {
	current, revision, err := planner.snapshot(ctx, environmentID)
	if err != nil {
		return groupstore.ReleaseGroupBlueprintPreparedMutation{}, false, err
	}
	previous := make([]ReleaseGroupIdentity, len(current))
	snapshot := make([]groupstore.ReleaseGroupSnapshotEntry, len(current))
	next := make([]domain.Group, len(current))
	indexes := make(map[string]int, len(current))
	for index, versioned := range current {
		previous[index] = ReleaseGroupIdentity{ID: versioned.Group.ID, Name: versioned.Group.Name}
		snapshot[index] = groupstore.ReleaseGroupSnapshotEntry{
			Group: versioned.Group, Revision: versioned.Revision, ReadRevision: versioned.ReadRevision,
		}
		next[index] = versioned.Group
		indexes[versioned.Group.ID] = index
	}
	reconciled, err := ReconcileBlueprintReleaseGroups(environmentID, specs, services, previous, allocate)
	if err != nil {
		return groupstore.ReleaseGroupBlueprintPreparedMutation{}, false, err
	}
	changed := false
	for _, group := range reconciled.Desired {
		index, found := indexes[group.ID]
		if !found {
			next = append(next, group)
			changed = true
			break
		}
		if !domain.Equal(next[index], group) {
			next[index] = group
			changed = true
			break
		}
	}
	if !changed && len(reconciled.Removed) > 0 {
		index := indexes[reconciled.Removed[0].ID]
		next = append(next[:index], next[index+1:]...)
		changed = true
	}
	prepared, err := planner.mutations.PrepareReleaseGroupBlueprintMutation(
		ctx, environmentID, revision, snapshot, next,
	)
	return prepared, changed, err
}
