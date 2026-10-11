package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareControllerServiceConfigurationSettlement records configuration without
// a host effect. The caller checks runtime intent and immutable deployment policy,
// and compares the exact runtime sidecar revision in the receipt's transaction.
func PrepareControllerServiceConfigurationSettlement(
	snapshot Snapshot,
	unit Unit,
	operationID string,
	projectionRevision, runtimeRevision int64,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindService || unit.Removal ||
		ids.Validate(ids.KindOperation, operationID) != nil || projectionRevision <= 0 || runtimeRevision < 0 ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Service unit changed")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{
		Kind: unit.Target.Kind, ID: unit.Target.ID,
	}) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Service unit is not ready")
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: Applied, Fingerprint: unit.Fingerprint,
		ParentTaskID:               snapshot.HeadTaskID,
		ControllerOperationID:      operationID,
		ControllerRuntimeRevision:  runtimeRevision,
		ControllerAuthoredRevision: projectionRevision,
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

func desiredControllerUnitMatches(units []Unit, candidate Unit) bool {
	for _, unit := range units {
		if unit.Target == candidate.Target {
			return unit.Removal == candidate.Removal && unit.Fingerprint == candidate.Fingerprint &&
				slices.Equal(unit.Reads, candidate.Reads) &&
				slices.Equal(unit.Writes, candidate.Writes) &&
				slices.Equal(unit.After, candidate.After)
		}
	}
	return false
}
