package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareControllerDisabledComponentSettlement records that the exact current
// disabled singleton already satisfies its explicitly authored unit. The
// caller must compare the captured Component and runtime revision (including
// runtime absence) in the same transaction as this receipt.
func PrepareControllerDisabledComponentSettlement(
	snapshot Snapshot,
	unit Unit,
	operationID string,
	componentRecordRevision int64,
	componentRuntimeRevision int64,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindComponent || unit.Removal ||
		ids.Validate(ids.KindOperation, operationID) != nil || componentRecordRevision <= 0 ||
		componentRuntimeRevision < 0 || len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Component unit changed")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{
		Kind: unit.Target.Kind, ID: unit.Target.ID,
	}) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Component unit is not ready")
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: Applied, Fingerprint: unit.Fingerprint,
		ParentTaskID:                       snapshot.HeadTaskID,
		ControllerOperationID:              operationID,
		ControllerComponentRecordRevision:  componentRecordRevision,
		ControllerComponentRuntimeRevision: componentRuntimeRevision,
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
