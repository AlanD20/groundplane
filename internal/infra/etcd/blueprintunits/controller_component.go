package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareControllerDisabledComponentSettlement captures the compared input
// revision of a proven-disabled singleton. The caller publishes its exact
// authored metadata in the same transaction as this receipt, retaining or
// retiring only a proven-empty runtime. The shared publication revision, not
// an invented Agent acknowledgement, binds the resulting metadata.
func PrepareControllerDisabledComponentSettlement(
	snapshot Snapshot,
	unit Unit,
	operationID string,
	componentRecordRevision int64,
	componentRuntimeRevision int64,
	proxySelection bool,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindComponent ||
		ids.Validate(ids.KindOperation, operationID) != nil || componentRecordRevision <= 0 ||
		componentRuntimeRevision < 0 || !validControllerComponentWrites(unit) {
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
	state := Applied
	if unit.Removal {
		state = Absent
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: state, Fingerprint: unit.Fingerprint,
		ParentTaskID:                       snapshot.HeadTaskID,
		ControllerOperationID:              operationID,
		ControllerComponentRecordRevision:  componentRecordRevision,
		ControllerComponentRuntimeRevision: componentRuntimeRevision,
		ControllerProxySelection:           proxySelection,
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

// Additional Service claims serialize the shared forwarding choice. This
// metadata receipt acknowledges only its Component target, never those
// Services' runtime. The coordinator separately requires their own receipts.
func validControllerComponentWrites(unit Unit) bool {
	if unit.Target.Kind != ids.KindComponent || !validUnit(unit) {
		return false
	}
	for _, write := range unit.Writes {
		if write != unit.Target && write.Kind != ids.KindService {
			return false
		}
	}
	return true
}

func ControllerDisabledComponentPublicationMatches(
	applied etcdstore.Versioned[AppliedRecord], componentRevision, runtimeRevision int64,
) bool {
	receipt := applied.Record
	return receipt.Target.Kind == ids.KindComponent && receipt.ControllerOperationID != "" &&
		receipt.Execution == nil && receipt.ControllerComponentRecordRevision > 0 &&
		receipt.ControllerComponentRecordRevision < applied.Revision && applied.Revision == componentRevision &&
		receipt.ControllerComponentRuntimeRevision == runtimeRevision
}
