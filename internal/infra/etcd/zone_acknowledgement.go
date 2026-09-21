package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareZoneRemovalAcknowledgement(
	ctx context.Context, task TaskRecord, terminalStatus taskjournal.TaskStatus, terminalAt time.Time, readRevision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	environmentID, operationID, err := zoneRemovalTaskIdentity(task)
	if err != nil {
		return nil, nil, err
	}
	keys := []string{
		deletionTombstoneKey(string(deletionrecord.DeletionTargetZone), task.Target),
		zonePoolRegistryKey(environmentID), componentAddressRegistryKey(task.Target),
		zoneRemovalIntentKey(operationID), environmentBlueprintHeadKey(environmentID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID), componentTaskActiveEnvironmentKey(environmentID),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return nil, nil, err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] == nil ||
		state.Values[1] == nil || state.Values[3] == nil || state.Values[4] == nil ||
		state.Values[5] == nil || state.Values[6] == nil {
		return nil, nil, errs.New(errs.KindInternal, "Zone removal state is inconsistent")
	}
	intent, err := decodeZoneRemovalIntent(state.Values[3].Value)
	if err != nil {
		return nil, nil, err
	}
	if err := validateZoneRemovalTaskOwner(task, intent); err != nil {
		return nil, nil, err
	}
	if intent.Status != taskjournal.TaskStatusPending || state.Values[4].ModRevision != intent.DesiredHeadRevision ||
		state.Values[5].ModRevision != intent.AppliedProjectionRevision || string(state.Values[6].Value) != task.ID {
		return nil, nil, errs.New(errs.KindStateConflict, "Zone removal terminal ownership changed")
	}
	headID, err := idempotencyrecord.DecodeTaskReference(state.Values[4].Value)
	if err != nil || headID != intent.DesiredProjection.RevisionID {
		return nil, nil, errs.New(errs.KindStateConflict, "Zone removal desired head changed")
	}
	applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[5].Value)
	if err != nil || !sameServiceRemovalProjection(applied, intent.AppliedProjection) {
		return nil, nil, errs.New(errs.KindStateConflict, "Zone removal applied projection changed")
	}
	zone, err := projectedZoneRemovalTarget(intent, readRevision)
	if err != nil {
		return nil, nil, err
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetZone || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != intent.ZoneRevision || tombstone.TaskID != task.ID ||
		(tombstone.Phase != deletionrecord.DeletionPhaseHostEffects && tombstone.Phase != deletionrecord.DeletionPhaseFinalizing) {
		return nil, nil, errs.New(errs.KindStateConflict, "Zone removal tombstone changed")
	}
	pool, err := recordcodec.Decode[zonePoolRegistry](state.Values[1].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(pool) != nil || pool.Reservations[task.Target] != zone.Desired.Subnet {
		return nil, nil, errs.New(errs.KindInternal, "Zone subnet reservation is inconsistent")
	}
	if state.Values[2] != nil {
		addresses, decodeErr := recordcodec.Decode[componentAddressRegistry](
			state.Values[2].Value,
			"component_address_registry",
		)
		if decodeErr != nil || validateComponentAddressRegistry(zone, addresses) != nil ||
			len(addresses.Reservations) != 0 {
			return nil, nil, errs.New(errs.KindResourceInUse, "Zone gained a Component address reservation")
		}
	}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: state.Values[0].ModRevision},
		{Key: keys[1], ModRevision: state.Values[1].ModRevision},
		{Key: keys[2], ModRevision: keyValueRevision(state.Values[2])},
		{Key: keys[3], ModRevision: state.Values[3].ModRevision},
		{Key: keys[4], ModRevision: state.Values[4].ModRevision},
		{Key: keys[5], ModRevision: state.Values[5].ModRevision},
		{Key: keys[6], ModRevision: state.Values[6].ModRevision},
	}
	if terminalStatus != taskjournal.TaskStatusCompleted {
		terminalIntent, transitionErr := terminalZoneRemovalIntent(intent, terminalStatus, terminalAt)
		if transitionErr != nil {
			return nil, nil, transitionErr
		}
		intentValue, encodeErr := encodeZoneRemovalIntent(terminalIntent)
		if encodeErr != nil {
			return nil, nil, encodeErr
		}
		return conditions, []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: keys[3], Value: intentValue},
			{Type: etcdstore.MutationDelete, Key: keys[0]},
			{Type: etcdstore.MutationDelete, Key: keys[6]},
		}, nil
	}
	hierarchy := &HierarchyRepository{store: repository.store}
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx, intent.Claim,
		EnvironmentDesiredRevisionIdentity{EnvironmentID: intent.EnvironmentID, RevisionID: intent.Claim.RevisionID},
		intent.CandidateProjection,
		idempotencyrecord.IdempotencyMarker{Locator: intent.Claim.Locator, Intent: intent.Claim.Intent}, intent.DesiredHeadRevision,
	)
	if err != nil {
		return nil, nil, err
	}
	headValue, err := idempotencyrecord.EncodeTaskReference(intent.Claim.RevisionID)
	if err != nil {
		clear(publication.publishedDescriptor)
		return nil, nil, err
	}
	projectionValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(intent.CandidateProjection)
	if err != nil {
		clear(publication.publishedDescriptor)
		clear(headValue)
		return nil, nil, err
	}
	nextPool, err := pool.release(zone)
	if err != nil {
		clear(publication.publishedDescriptor)
		clear(headValue)
		clear(projectionValue)
		return nil, nil, err
	}
	conditions = append(
		conditions,
		etcdstore.Condition{
			Key:         environmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
			ModRevision: publication.rootRevision,
		},
		etcdstore.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		etcdstore.Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: etcdstore.MutationDelete, Key: publication.locatorKey},
		{Type: etcdstore.MutationPut, Key: keys[4], Value: headValue},
		{Type: etcdstore.MutationPut, Key: keys[5], Value: projectionValue},
		{Type: etcdstore.MutationDelete, Key: keys[3]},
		{Type: etcdstore.MutationDelete, Key: keys[0]},
		{Type: etcdstore.MutationDelete, Key: keys[6]},
	}
	if state.Values[2] != nil {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[2]})
	}
	if len(nextPool.Reservations) == 0 {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[1]})
	} else {
		poolValue, encodeErr := recordcodec.Encode("zone_pool_registry", nextPool)
		if encodeErr != nil {
			clearMutationValues(mutations)
			return nil, nil, encodeErr
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[1], Value: poolValue})
	}
	return conditions, mutations, nil
}

func (repository *TaskRepository) validateZoneRemovalReplay(
	ctx context.Context, task TaskRecord, terminalStatus taskjournal.TaskStatus, readRevision int64,
) error {
	environmentID, operationID, err := zoneRemovalTaskIdentity(task)
	if err != nil {
		return err
	}
	keys := []string{
		deletionTombstoneKey(string(deletionrecord.DeletionTargetZone), task.Target),
		zonePoolRegistryKey(environmentID), zoneRemovalIntentKey(operationID),
		environmentBlueprintHeadKey(environmentID), projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID),
		componentTaskActiveEnvironmentKey(environmentID),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] != nil ||
		state.Values[3] == nil || state.Values[4] == nil || state.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "Zone removal terminal state does not match its Task")
	}
	applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[4].Value)
	if err != nil {
		return err
	}
	headID, err := idempotencyrecord.DecodeTaskReference(state.Values[3].Value)
	if err != nil {
		return err
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if state.Values[2] != nil || headID != task.Params[EnvironmentDesiredRevisionParam] ||
			applied.RevisionID != headID || projectionContainsZone(applied, task.Target) {
			return errs.New(errs.KindStateConflict, "completed Zone removal retained old desired state")
		}
		if state.Values[1] != nil {
			pool, poolErr := recordcodec.Decode[zonePoolRegistry](state.Values[1].Value, "zone_pool_registry")
			if poolErr != nil || validateZonePoolRegistry(pool) != nil {
				return errs.New(errs.KindInternal, "Zone pool registry is inconsistent")
			}
			if _, retained := pool.Reservations[task.Target]; retained {
				return errs.New(errs.KindStateConflict, "completed Zone removal retained its subnet")
			}
		}
		return nil
	}
	if state.Values[1] == nil || state.Values[2] == nil {
		return errs.New(errs.KindStateConflict, "failed Zone removal lost durable state")
	}
	intent, err := decodeZoneRemovalIntent(state.Values[2].Value)
	if err != nil || validateZoneRemovalTaskOwner(task, intent) != nil || intent.Status != terminalStatus ||
		intent.TerminalAt == nil || task.FinishedAt == nil || !intent.TerminalAt.Equal(*task.FinishedAt) ||
		state.Values[3].ModRevision != intent.DesiredHeadRevision || headID != intent.DesiredProjection.RevisionID ||
		state.Values[4].ModRevision != intent.AppliedProjectionRevision ||
		!sameServiceRemovalProjection(applied, intent.AppliedProjection) {
		return errs.New(errs.KindStateConflict, "failed Zone removal changed sealed desired state")
	}
	zone, err := projectedZoneRemovalTarget(intent, readRevision)
	if err != nil {
		return err
	}
	pool, err := recordcodec.Decode[zonePoolRegistry](state.Values[1].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(pool) != nil || pool.Reservations[task.Target] != zone.Desired.Subnet {
		return errs.New(errs.KindStateConflict, "failed Zone removal changed its subnet reservation")
	}
	return nil
}

func projectedZoneRemovalTarget(intent ZoneRemovalIntent, readRevision int64) (zonerecord.Record, error) {
	projection := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: intent.DesiredProjection, Revision: intent.DesiredHeadRevision, ReadRevision: readRevision,
	}
	var matched *zonerecord.Record
	for _, desired := range intent.DesiredProjection.DesiredZones {
		if desired.Desired.ID != intent.ZoneID {
			continue
		}
		if matched != nil {
			return zonerecord.Record{}, errs.New(errs.KindInternal, "Zone removal projection has duplicate target")
		}
		joined, err := joinEnvironmentZone(projection, desired)
		if err != nil {
			return zonerecord.Record{}, err
		}
		matched = &joined.Record
	}
	if matched == nil || matched.EnvironmentID != intent.EnvironmentID || matched.Desired.Name != intent.ZoneName ||
		intent.ZoneRevision != intent.DesiredHeadRevision {
		return zonerecord.Record{}, errs.New(errs.KindStateConflict, "Zone removal projected target changed")
	}
	return *matched, nil
}

func projectionContainsZone(projection projectionrecord.EnvironmentComposeProjection, zoneID string) bool {
	for _, desired := range projection.DesiredZones {
		if desired.Desired.ID == zoneID {
			return true
		}
	}
	return false
}

func zoneRemovalTaskIdentity(task TaskRecord) (string, string, error) {
	environmentID := task.Params[TaskZoneEnvironmentParam]
	operationID := task.Params[TaskZoneRemovalOperationParam]
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindNetwork, task.Target) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return "", "", errs.New(errs.KindInternal, "durable Zone removal Task shape is invalid")
	}
	return environmentID, operationID, nil
}
