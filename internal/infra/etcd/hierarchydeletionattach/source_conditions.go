package hierarchydeletionattach

import (
	"context"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func SourceConditions(
	ctx context.Context,
	store Reader,
	input Input,
) ([]keyvalue.Condition, error) {
	keys := []string{attachrecord.AttachKey(input.Attach.ID), attachrecord.AttachFactsKey(input.Attach.ID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil ||
		read.Values[0].ModRevision != input.AttachRevision ||
		keyvalue.RevisionOf(read.Values[1]) != input.FactsRevision {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "hierarchy Attach cleanup source changed")
	}
	defer keyvalue.ClearValues(read.Values)
	exclusion, err := attachrecord.RequireAttachBackupSourceExclusionAbsent(
		ctx,
		store,
		input.Attach.ID,
		read.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	projection, found, err := blueprints.ReadCurrentProjection(
		ctx,
		store,
		input.Attach.BackingEnvironmentID,
		read.ReadRevision,
	)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "hierarchy Attach backing desired state is absent")
	}
	backing, err := servicerecord.ReadJoined(ctx, store, servicerecord.DesiredSelection{
		Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: read.ReadRevision,
	}, input.Attach.BackingServiceID, blueprints.EnvironmentBlueprintHeadKey(input.Attach.BackingEnvironmentID))
	if err != nil {
		return nil, err
	}
	if !SameBackingDesired(input.Backing, backing.Record) ||
		backing.Record.Runtime.RuntimeIntent != "running" {
		return nil, errs.New(errs.KindStateConflict, "hierarchy Attach backing Service changed or is not runnable")
	}
	return []keyvalue.Condition{
		{Key: keys[0], ModRevision: input.AttachRevision},
		{Key: keys[1], ModRevision: input.FactsRevision},
		exclusion,
		servicerecord.ServiceDesiredCondition(backing),
		servicerecord.ServiceRuntimeCondition(backing),
		{Key: attachrecord.AttachGrantedByPrefix(input.Attach.ID), Prefix: true},
		{Key: attachrecord.AttachCredentialByPrefix(input.Attach.ID), Prefix: true},
		{Key: deletions.TombstoneKey("attach", input.Attach.ID)},
		{Key: deletions.TombstoneKey("project", input.Attach.BackingProjectID)},
		{Key: deletions.TombstoneKey("environment", input.Attach.BackingEnvironmentID)},
		{Key: deletions.TombstoneKey("service", input.Attach.BackingServiceID)},
		{Key: deletions.TombstoneKey("zone", input.Attach.BackingNetworkID)},
	}, nil
}
