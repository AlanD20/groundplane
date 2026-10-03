package hierarchydeletionattach

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ReadCaptured(
	ctx context.Context,
	store Reader,
	snapshotRevision int64,
	parentOperationID, attachID string,
) (Input, error) {
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{
			attachrecord.AttachKey(attachID),
			attachrecord.AttachFactsKey(attachID),
		}, Revision: snapshotRevision,
	})
	if err != nil {
		return Input{}, err
	}
	if read == nil || read.ReadRevision != snapshotRevision || len(read.Values) != 2 || read.Values[0] == nil {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	attach, err := attachrecord.DecodeAttachRecord(read.Values[0].Value)
	if err != nil || attach.ID != attachID {
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	input := Input{Schema: 1, ParentOperationID: parentOperationID, SnapshotRevision: snapshotRevision,
		ActionOrdinal: -1, Attach: attach, AttachRevision: read.Values[0].ModRevision}
	if read.Values[1] != nil {
		facts, err := attachrecord.DecodeAttachEncryptedFacts(read.Values[1].Value)
		if err != nil {
			return Input{}, err
		}
		input.Facts, input.FactsRevision = &facts, read.Values[1].ModRevision
	}
	projection, found, err := blueprints.ReadCurrentProjection(
		ctx,
		store,
		attach.BackingEnvironmentID,
		snapshotRevision,
	)
	if err != nil || !found {
		Clear(&input)
		if err != nil {
			return Input{}, err
		}
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	backing, err := servicerecord.ReadJoined(ctx, store, servicerecord.DesiredSelection{
		Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: snapshotRevision,
	}, attach.BackingServiceID, blueprints.EnvironmentBlueprintHeadKey(attach.BackingEnvironmentID))
	if err != nil {
		Clear(&input)
		return Input{}, err
	}
	input.Backing, input.BackingDesiredRevision = backing.Record, backing.Revision
	input.BackingRevision = servicerecord.ServiceRuntimeRevision(backing)
	owners, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys:     []string{hierarchy.EnvironmentKey(attach.EnvironmentID)},
			Revision: snapshotRevision,
		},
	)
	if err != nil {
		Clear(&input)
		return Input{}, err
	}
	if owners == nil || owners.ReadRevision != snapshotRevision || len(owners.Values) != 1 || owners.Values[0] == nil {
		Clear(&input)
		if owners != nil {
			keyvalue.ClearValues(owners.Values)
		}
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	environment, err := hierarchy.DecodeEnvironment(owners.Values[0].Value)
	keyvalue.ClearValues(owners.Values)
	if err != nil || environment.ID != attach.EnvironmentID {
		Clear(&input)
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	projects, err := store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys:     []string{hierarchy.ProjectKey(environment.ProjectID)},
			Revision: snapshotRevision,
		},
	)
	if err != nil {
		Clear(&input)
		return Input{}, err
	}
	if projects == nil || projects.ReadRevision != snapshotRevision || len(projects.Values) != 1 ||
		projects.Values[0] == nil {
		Clear(&input)
		if projects != nil {
			keyvalue.ClearValues(projects.Values)
		}
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	project, err := hierarchy.DecodeProject(projects.Values[0].Value)
	keyvalue.ClearValues(projects.Values)
	if err != nil || project.ID != environment.ProjectID {
		Clear(&input)
		return Input{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	input.ProjectID, input.TenantID = project.ID, project.TenantID
	if err := Validate(input); err != nil {
		Clear(&input)
		return Input{}, err
	}
	return input, nil
}

func NeedsAgent(input Input) bool {
	if !input.Attach.OwnsCredential() || input.Attach.Status == "detached" {
		return false
	}
	if input.Backing.Desired.Adapter == "custom" {
		return input.Backing.Desired.Hooks != nil && input.Backing.Desired.Hooks.Detach != nil
	}
	return input.Backing.Desired.Authentication != "none"
}

func RequireTerminal(input Input) error {
	if input.Attach.Status != "ready" && input.Attach.Status != "failed" && input.Attach.Status != "detached" {
		return errs.New(errs.KindResourceInUse, "Attach operation is not terminal")
	}
	if NeedsAgent(input) && (input.Facts == nil || input.Backing.Runtime.RuntimeIntent != "running") {
		return errs.New(errs.KindStateConflict, "Attach cleanup requires captured facts and a runnable backing Service")
	}
	return nil
}
