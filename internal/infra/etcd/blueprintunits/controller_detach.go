package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerDetachReceipt retains exact input authority for a proved absence
// or metadata-only binding removal. It is never an Agent execution receipt.
type ControllerDetachReceipt struct {
	CaptureRevision int64               `json:"capture_revision"`
	Input           workexecution.Input `json:"input"`
}

func validControllerDetach(record AppliedRecord) bool {
	proof := record.ControllerDetach
	return proof != nil && record.Target.Kind == ids.KindAttach && record.State == Absent && record.Fingerprint == "" &&
		proof.CaptureRevision > 0 && proof.Input.Validate() == nil &&
		ids.Validate(
			ids.KindTask,
			record.ParentTaskID,
		) == nil && ids.Validate(ids.KindOperation, record.ControllerOperationID) == nil &&
		record.Execution == nil && record.NativeOperationID == "" && record.MetadataDNS == nil &&
		record.ControllerAttachInputRevision == 0 && record.ControllerAttachBindingSHA256 == "" && len(record.ControllerAttachReferences) == 0 &&
		record.ControllerValueGenerationID == "" && record.ControllerRuntimeRevision == 0 && record.ControllerAuthoredRevision == 0 &&
		record.ControllerComponentRecordRevision == 0 && record.ControllerComponentRuntimeRevision == 0 &&
		!record.ControllerProxySelection && record.ControllerScriptSetRevision == 0 && record.ControllerDNS == nil &&
		record.ControllerServiceDeclarationAbsence == nil && len(record.AffectedWrites) == 0
}

// The caller must atomically join positive declaration absence or native
// binding deletion, the exact captured input and work settlement to this plan.
func PrepareControllerDetachRemoval(snapshot Snapshot, unit Unit, operationID string,
	captureRevision int64, input workexecution.Input,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindAttach || !unit.Removal || !slices.Contains(unit.Writes, unit.Target) ||
		captureRevision <= 0 || captureRevision > snapshot.ReadRevision ||
		!desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Detach receipt has no matching Blueprint removal")
	}
	applied := AppliedRecord{EnvironmentID: snapshot.EnvironmentID, Target: unit.Target, State: Absent,
		ParentTaskID: snapshot.HeadTaskID, ControllerOperationID: operationID,
		ControllerDetach: &ControllerDetachReceipt{CaptureRevision: captureRevision, Input: input}}
	if !validControllerDetach(applied) {
		return MutationPlan{}, invalidRecord()
	}
	plan, err := PrepareMutation(snapshot, []AppliedChange{{Target: unit.Target, Next: &applied}}, nil)
	if err != nil {
		return MutationPlan{}, err
	}
	plan.conditions = append(
		plan.conditions,
		keyvalue.Condition{Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision},
	)
	return plan, nil
}

func validDetachWrites(unit Unit) bool {
	if !unit.Removal || unit.Target.Kind != ids.KindAttach || !slices.Contains(unit.Writes, unit.Target) ||
		len(unit.Writes) > 2 {
		return false
	}
	for _, target := range unit.Writes {
		if target != unit.Target && target.Kind != ids.KindService {
			return false
		}
	}
	return true
}
