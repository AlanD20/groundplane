package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DeclarationAbsenceConditions proves that a newborn identity never acquired
// acknowledged effects or another executor. The caller must also fence actual
// resource/value absence; birth alone does not authorize removal of live state.
func DeclarationAbsenceConditions(snapshot Snapshot, target ResourceKey,
	birthRevisionID, workID string,
) ([]keyvalue.Condition, bool, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		!validKey(target) || ids.Validate(ids.KindTask, birthRevisionID) != nil ||
		ids.Validate(ids.KindWorkItem, workID) != nil {
		return nil, false, invalidRecord()
	}
	initial := false
	for _, applied := range snapshot.Applied {
		if applied.Record.Target == target {
			initial = applied.Revision == 0 && applied.Record.State == Absent && !applied.Record.HasEffectSource()
		}
	}
	if !initial {
		return nil, false, nil
	}
	for _, execution := range snapshot.Executions {
		if !slices.Contains(execution.Record.Unit.Writes, target) {
			continue
		}
		if execution.Record.TaskID != snapshot.HeadTaskID || execution.Record.WorkItemID != workID ||
			execution.Record.State != Pending || execution.Record.AttemptNumber != 0 || execution.Record.Executor != nil ||
			execution.Record.AttemptID != "" {
			return nil, false, nil
		}
	}
	for _, birth := range snapshot.initialAbsence {
		if birth.Record.BirthRevisionID == birthRevisionID && slices.Contains(birth.Record.Targets, target) &&
			birth.Revision > 0 {
			return []keyvalue.Condition{
				{Key: initialAbsenceKey(snapshot.EnvironmentID, birthRevisionID), ModRevision: birth.Revision},
				{Key: EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
				{Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision},
			}, true, nil
		}
	}
	return nil, false, errs.New(errs.KindStateConflict, "Resource birth absence is unavailable")
}
