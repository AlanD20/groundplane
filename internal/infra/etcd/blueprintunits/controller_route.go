package blueprintunits

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The Route receipt compares the immutable authored root in one transaction.
// Ensure settles metadata before router execution; removal requires the caller
// to join exact acknowledged withdrawal and metadata retirement in that commit.
func prepareControllerRouteReceipt(
	snapshot Snapshot,
	unit Unit,
	operationID string,
	authoredRevision int64,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindRoute ||
		ids.Validate(ids.KindOperation, operationID) != nil || authoredRevision <= 0 ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Route unit changed")
	}
	state := Applied
	if unit.Removal {
		state = Absent
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: state, Fingerprint: unit.Fingerprint,
		ParentTaskID:               snapshot.HeadTaskID,
		ControllerOperationID:      operationID,
		ControllerAuthoredRevision: authoredRevision,
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
