package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A Route's authored metadata is already in the immutable runtime projection.
// Its Controller receipt must compare that exact projection in the same
// transaction; the HTTP router Component owns any later host configuration.
func PrepareControllerRouteSettlement(
	snapshot Snapshot,
	unit Unit,
	operationID string,
	projectionRevision int64,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindRoute || unit.Removal ||
		ids.Validate(ids.KindOperation, operationID) != nil || projectionRevision <= 0 ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Route unit changed")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{
		Kind: unit.Target.Kind, ID: unit.Target.ID,
	}) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Route unit is not ready")
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: Applied, Fingerprint: unit.Fingerprint,
		ParentTaskID:                 snapshot.HeadTaskID,
		ControllerOperationID:        operationID,
		ControllerProjectionRevision: projectionRevision,
	}
	plan, err := PrepareMutation(snapshot, []AppliedChange{{Target: unit.Target, Next: &applied}}, nil)
	if err != nil {
		return MutationPlan{}, err
	}
	plan.conditions = append(plan.conditions, etcdstore.Condition{
		Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision,
	})
	return plan, nil
}
