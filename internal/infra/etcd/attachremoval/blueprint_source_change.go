package attachremoval

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// BlueprintSourceChange proves a contradiction to admission authority. The
// returned actual comparisons must join rejection. Consumer runtime/epoch may
// advance through captured prerequisites and are checked by their own owner;
// they cannot be classified as source rejection by a blanket revision check.
func (repository *InputRepository) BlueprintSourceChange(ctx context.Context, view workqueue.View,
	frozen BlueprintInput,
) (bool, []keyvalue.Condition, error) {
	ignored := []string{
		blueprintunits.EpochKey(frozen.EnvironmentID),
		blueprintunits.DesiredPlanKey(frozen.EnvironmentID),
	}
	if frozen.Snapshot != nil {
		for _, condition := range frozen.Snapshot.ConsumerConditions() {
			ignored = append(ignored, condition.Key)
		}
	}
	for _, expected := range frozen.Conditions {
		if slices.Contains(ignored, expected.Key) {
			continue
		}
		if expected.Prefix {
			read, err := repository.store.Range(
				ctx,
				keyvalue.RangeRequest{Prefix: expected.Key, Limit: 1, Revision: view.ReadRevision},
			)
			if err != nil {
				return false, nil, err
			}
			if read == nil || read.ReadRevision != view.ReadRevision {
				return false, nil, checkpointConflict("Detach source comparison snapshot is incomplete")
			}
			if len(read.Values) != 0 {
				actual := keyvalue.Condition{Key: read.Values[0].Key, ModRevision: read.Values[0].ModRevision}
				keyvalue.ClearRangeValues(read.Values)
				return true, []keyvalue.Condition{actual}, nil
			}
			if read.More {
				return false, nil, checkpointConflict("Detach source comparison returned an invalid range")
			}
			continue
		}
		read, err := repository.read(ctx, []string{expected.Key}, view.ReadRevision)
		if err != nil {
			return false, nil, err
		}
		actual := keyvalue.RevisionOf(read.Values[0])
		keyvalue.ClearValues(read.Values)
		if actual != expected.ModRevision {
			return true, []keyvalue.Condition{{Key: expected.Key, ModRevision: actual}}, nil
		}
	}
	return false, nil, nil
}
