package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareControllerScriptSettlement acknowledges metadata in the parent's
// exact active Script set. The caller must atomically compare that set and the
// selected Script/body, or their absence. Hook execution is a Service effect.
func PrepareControllerScriptSettlement(
	snapshot Snapshot, unit Unit, operationID string, scriptSetRevision int64,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindScript || ids.Validate(ids.KindOperation, operationID) != nil ||
		scriptSetRevision <= 0 || len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Script unit changed")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{Kind: unit.Target.Kind, ID: unit.Target.ID}) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Blueprint Script unit is not ready")
	}
	state := Applied
	if unit.Removal {
		state = Absent
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target,
		State: state, Fingerprint: unit.Fingerprint, ParentTaskID: snapshot.HeadTaskID,
		ControllerOperationID: operationID, ControllerScriptSetRevision: scriptSetRevision,
	}
	plan, err := PrepareMutation(snapshot, []AppliedChange{{Target: unit.Target, Next: &applied}}, nil)
	if err != nil {
		return MutationPlan{}, err
	}
	plan.conditions = append(plan.conditions, keyvalue.Condition{
		Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision,
	})
	return plan, nil
}
