package blueprintunits

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core/blueprintreconcile"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func PrepareControllerAttachBinding(
	snapshot Snapshot, unit Unit, operationID string, inputRevision int64, bindingSHA256 string, references []attachments.BindingReference,
) (MutationPlan, error) {
	if snapshot.Desired == nil || snapshot.Desired.Record.ParentTaskID != snapshot.HeadTaskID ||
		unit.Target.Kind != ids.KindAttach || unit.Removal || inputRevision <= 0 || inputRevision > snapshot.ReadRevision ||
		!recordcodec.ValidSHA256(bindingSHA256) ||
		ids.Validate(ids.KindOperation, operationID) != nil || len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return MutationPlan{}, invalidRecord()
	}
	if !desiredControllerUnitMatches(snapshot.Desired.Record.Units, unit) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Attach binding is not selected by this Blueprint")
	}
	selection, err := Select(snapshot)
	if err != nil {
		return MutationPlan{}, err
	}
	if !slices.Contains(selection.Ready, blueprintreconcile.ResourceKey{Kind: unit.Target.Kind, ID: unit.Target.ID}) {
		return MutationPlan{}, errs.New(errs.KindStateConflict, "Attach binding dependencies have not settled")
	}
	receipt := AppliedRecord{
		EnvironmentID: snapshot.EnvironmentID, Target: unit.Target, State: Applied, Fingerprint: unit.Fingerprint,
		ParentTaskID: snapshot.HeadTaskID, ControllerOperationID: operationID, ControllerAttachInputRevision: inputRevision,
		ControllerAttachBindingSHA256: bindingSHA256,
		ControllerAttachReferences:    append([]attachments.BindingReference(nil), references...),
	}
	plan, err := PrepareMutation(snapshot, []AppliedChange{{Target: unit.Target, Next: &receipt}}, nil)
	if err != nil {
		return MutationPlan{}, err
	}
	plan.conditions = append(plan.conditions, etcdstore.Condition{
		Key: DesiredPlanKey(snapshot.EnvironmentID), ModRevision: snapshot.Desired.Revision,
	})
	return plan, nil
}
