package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceDeclarationAbsence records retirement of an unexecuted declaration.
// Birth is positive publication evidence, not an inferred missing runtime.
// The caller compares the declaration, birth, runtime absence and live references
// atomically with this receipt. No Agent result or applied projection is invented.
type ServiceDeclarationAbsence struct {
	DeclarationRevisionID string `json:"declaration_revision_id"`
	DeclarationRevision   int64  `json:"declaration_revision"`
	BirthRevisionID       string `json:"birth_revision_id"`
	BirthRevision         int64  `json:"birth_revision"`
}

func validServiceDeclarationAbsence(record AppliedRecord) bool {
	proof := record.ControllerServiceDeclarationAbsence
	return proof != nil && record.Target.Kind == ids.KindService && record.State == Absent &&
		record.ControllerOperationID != "" &&
		ids.Validate(ids.KindTask, proof.DeclarationRevisionID) == nil && proof.DeclarationRevision > 0 &&
		ids.Validate(ids.KindTask, proof.BirthRevisionID) == nil && proof.BirthRevision > 0
}

// TryPrepareControllerServiceDeclarationRemoval declines metadata settlement
// unless the ledger still contains its original birth absence and no execution
// can write this Service. Host effects retain the normal Agent removal path.
func TryPrepareControllerServiceDeclarationRemoval(
	snapshot Snapshot, unit Unit, operationID, declarationRevisionID string,
	declarationRevision int64, birthRevisionID string,
) (MutationPlan, bool, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindService || !unit.Removal ||
		ids.Validate(ids.KindOperation, operationID) != nil ||
		ids.Validate(ids.KindTask, declarationRevisionID) != nil || declarationRevision <= 0 ||
		ids.Validate(ids.KindTask, birthRevisionID) != nil ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, false, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, false, errs.New(errs.KindStateConflict, "Blueprint Service omission unit changed")
	}
	initial := false
	for _, applied := range snapshot.Applied {
		if applied.Record.Target == unit.Target {
			initial = applied.Revision == 0 && applied.Record.State == Absent && !applied.Record.HasEffectSource()
		}
	}
	if !initial {
		return MutationPlan{}, false, nil
	}
	for _, execution := range snapshot.Executions {
		if slices.Contains(execution.Record.Unit.Writes, unit.Target) {
			return MutationPlan{}, false, nil
		}
	}
	var birthRevision int64
	for _, birth := range snapshot.initialAbsence {
		if birth.Record.BirthRevisionID == birthRevisionID && slices.Contains(birth.Record.Targets, unit.Target) {
			birthRevision = birth.Revision
		}
	}
	if birthRevision <= 0 {
		return MutationPlan{}, false, errs.New(errs.KindStateConflict, "Blueprint Service birth absence is unavailable")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, false, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{Kind: unit.Target.Kind, ID: unit.Target.ID}) {
		return MutationPlan{}, false, errs.New(errs.KindStateConflict, "Blueprint Service omission is not ready")
	}
	applied := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target, State: Absent,
		ParentTaskID: snapshot.HeadTaskID, ControllerOperationID: operationID,
		ControllerServiceDeclarationAbsence: &ServiceDeclarationAbsence{
			DeclarationRevisionID: declarationRevisionID, DeclarationRevision: declarationRevision,
			BirthRevisionID: birthRevisionID, BirthRevision: birthRevision,
		},
	}
	plan, err := PrepareMutation(snapshot, []AppliedChange{{Target: unit.Target, Next: &applied}}, nil)
	if err != nil {
		return MutationPlan{}, false, err
	}
	plan.conditions = append(plan.conditions,
		keyvalue.Condition{Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision},
		keyvalue.Condition{Key: initialAbsenceKey(snapshot.EnvironmentID, birthRevisionID), ModRevision: birthRevision},
	)
	return plan, true, nil
}
