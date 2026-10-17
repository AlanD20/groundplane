package attachremoval

import (
	"context"

	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type SourceReader interface {
	Reader
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

func SourceConditions(
	ctx context.Context,
	store SourceReader,
	input Input,
) ([]keyvalue.Condition, error) {
	if err := Validate(input); err != nil {
		return nil, err
	}
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
		return nil, errs.New(errs.KindStateConflict, "Attach cleanup source changed")
	}
	defer keyvalue.ClearValues(read.Values)
	for _, prefix := range []string{attachrecord.AttachGrantedByPrefix(input.Attach.ID), attachrecord.AttachCredentialByPrefix(input.Attach.ID)} {
		dependents, err := store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 1, Revision: read.ReadRevision})
		if err != nil {
			return nil, err
		}
		if dependents == nil || dependents.ReadRevision != read.ReadRevision {
			return nil, errs.New(errs.KindStateConflict, "Attach dependency snapshot is unavailable")
		}
		inUse := len(dependents.Values) != 0 || dependents.More
		keyvalue.ClearRangeValues(dependents.Values)
		if inUse {
			return nil, errs.New(errs.KindResourceInUse, "remove dependent Attaches and grants before detaching their credential owner")
		}
	}
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
		return nil, errs.New(errs.KindStateConflict, "Attach backing desired state is absent")
	}
	backing, err := servicerecord.ReadJoined(ctx, store, servicerecord.DesiredSelection{
		Services: projection.Record.DesiredServices, Revision: projection.Revision, ReadRevision: read.ReadRevision,
	}, input.Attach.BackingServiceID, blueprints.EnvironmentBlueprintHeadKey(input.Attach.BackingEnvironmentID))
	if err != nil {
		return nil, err
	}
	if !SameBackingDesired(input.Backing, backing.Record) ||
		NeedsAgent(input) && backing.Record.Runtime.RuntimeIntent != "running" {
		return nil, errs.New(errs.KindStateConflict, "Attach backing Service changed or is not runnable")
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
