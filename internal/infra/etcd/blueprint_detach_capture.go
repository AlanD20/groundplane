package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CaptureBlueprintDetach freezes removal authority while admission can still
// refuse the Apply. Generated facts and hooks are never selected from later
// backing configuration by the background input preparer.
func (repository *EnvironmentBlueprintRepository) CaptureBlueprintDetach(ctx context.Context,
	parent workexecution.Task, unit blueprintunits.Unit, owned environmentprojection.EnvironmentOwnedIdentities,
	snapshot blueprintunits.Snapshot,
) (attachremoval.BlueprintInput, error) {
	if repository == nil || parent.Validate() != nil || parent.Action != workexecution.ActionApply ||
		parent.PublicationComplete || parent.Scope.EnvironmentID != snapshot.EnvironmentID ||
		owned.EnvironmentID != snapshot.EnvironmentID || owned.RevisionID != parent.ID ||
		!unit.Removal || unit.Target.Kind != ids.KindAttach {
		return attachremoval.BlueprintInput{}, errs.New(
			errs.KindValidationFailed,
			"Detach capture requires unpublished Apply removal work",
		)
	}
	source, err := blueprints.ReadOmissionSource(ctx, repository.store, owned, ids.KindAttach,
		unit.Target.ID, snapshot.ReadRevision)
	if err != nil {
		return attachremoval.BlueprintInput{}, err
	}
	spec, declared := source.Desired.Input.Attachments[source.Identity.Name]
	if !declared {
		return attachremoval.BlueprintInput{}, errs.New(
			errs.KindStateConflict,
			"Removed Attach declaration is unavailable",
		)
	}
	consumerID := ""
	for _, consumer := range source.Identities.Services {
		if consumer.Name == spec.Service {
			consumerID = consumer.ID
		}
	}
	if consumerID == "" {
		return attachremoval.BlueprintInput{}, errs.New(
			errs.KindStateConflict,
			"Removed Attach consumer identity is unavailable",
		)
	}
	workID := ids.DeriveAt(ids.KindWorkItem, parent.CreatedAt, parent.ID,
		"blueprint-resource/"+string(ids.KindAttach)+"/"+unit.Target.ID)
	input := attachremoval.BlueprintInput{TaskID: parent.ID, WorkItemID: workID,
		EnvironmentID: parent.Scope.EnvironmentID, AttachName: source.Identity.Name, AttachID: unit.Target.ID, ConsumerServiceID: consumerID,
		DeclarationRevisionID: source.Desired.RevisionID, BirthRevisionID: source.Identity.BirthRevisionID,
		OperationID: attachremoval.DetachOperation(workqueue.View{Task: workqueue.TaskSnapshot{Task: parent},
			Item: workqueue.ItemSnapshot{Item: workexecution.Item{ID: workID}}}), Conditions: source.Conditions}
	keys := []string{attachments.AttachKey(unit.Target.ID), attachments.AttachFactsKey(unit.Target.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return attachremoval.BlueprintInput{}, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return attachremoval.BlueprintInput{}, errs.New(errs.KindInternal, "Detach admission read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, key := range keys {
		if value := read.Values[index]; value != nil && (value.Key != key || value.ModRevision <= 0) {
			return attachremoval.BlueprintInput{}, errs.New(errs.KindInternal, "Detach admission read is inconsistent")
		}
		input.Conditions = append(
			input.Conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	if read.Values[0] == nil {
		guards, absent, err := blueprintunits.DeclarationAbsenceConditions(snapshot, unit.Target,
			source.Identity.BirthRevisionID, workID)
		if err != nil {
			return attachremoval.BlueprintInput{}, err
		}
		if !absent || read.Values[1] != nil {
			return attachremoval.BlueprintInput{}, errs.New(
				errs.KindStateConflict,
				"Missing Attach has no unprovisioned declaration proof",
			)
		}
		input.Conditions = append(input.Conditions, guards...)
		return input, nil
	}
	captured, err := attachremoval.ReadDetachSnapshot(ctx, repository.store, snapshot.ReadRevision, unit.Target.ID)
	if err != nil {
		return attachremoval.BlueprintInput{}, err
	}
	input.Snapshot = &captured
	if captured.Source.Attach.ServiceID != consumerID {
		input.Clear()
		return attachremoval.BlueprintInput{}, errs.New(errs.KindStateConflict, "Removed Attach changed its consumer")
	}
	guards, err := attachremoval.CaptureSourceConditions(ctx, repository.store, captured.Source)
	if err != nil {
		input.Clear()
		return attachremoval.BlueprintInput{}, err
	}
	input.Conditions = append(input.Conditions, guards...)
	input.Conditions = append(input.Conditions, captured.ConsumerConditions()...)
	return input, nil
}

func (repository *EnvironmentBlueprintRepository) ReadBlueprintOmissionSource(ctx context.Context,
	owned environmentprojection.EnvironmentOwnedIdentities, kind ids.Kind, id string, revision int64,
) (blueprints.OmissionSource, error) {
	return blueprints.ReadOmissionSource(ctx, repository.store, owned, kind, id, revision)
}
