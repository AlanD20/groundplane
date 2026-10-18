package blueprintunits

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Entry decision admission owns dependency and conflict fencing. The caller
// joins this resource receipt with the native work result and exact value writes.
func prepareControllerEntryReceipt(
	snapshot Snapshot, unit Unit, operationID, valueGenerationID string,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindEnvEntry ||
		ids.Validate(ids.KindOperation, operationID) != nil ||
		ids.Validate(ids.KindConfig, valueGenerationID) != nil ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Entry unit is not desired")
	}
	state := Applied
	if unit.Removal {
		state = Absent
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: state, Fingerprint: unit.Fingerprint,
		ParentTaskID:                snapshot.HeadTaskID,
		ControllerOperationID:       operationID,
		ControllerValueGenerationID: valueGenerationID,
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
