package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	environmentqueries "github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareZoneRemovalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	readRevision int64,
	assignment taskassignments.TaskAssignmentRecord,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	environmentID, operationID, err := zoneRemovalTaskIdentity(task)
	if err != nil {
		return nil, nil, err
	}
	keys := []string{
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), task.Target),
		networkreservations.ZonePoolRegistryKey(
			environmentID,
		), networkreservations.ComponentAddressRegistryKey(task.Target),
		environmentchanges.ZoneRemovalIntentKey(operationID), blueprints.EnvironmentBlueprintHeadKey(environmentID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(
			environmentID,
		), environmentchanges.ComponentTaskActiveEnvironmentKey(environmentID),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return nil, nil, err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] == nil ||
		state.Values[1] == nil || state.Values[3] == nil || state.Values[6] == nil {
		return nil, nil, errs.New(errs.KindInternal, "Zone removal state is inconsistent")
	}
	intent, err := environmentchanges.DecodeZoneRemovalIntent(state.Values[3].Value)
	if err != nil {
		return nil, nil, err
	}
	if err := validateZoneRemovalTaskOwner(task, intent); err != nil {
		return nil, nil, err
	}
	if intent.Status != taskjournal.TaskStatusPending || !environmentchanges.ZoneRemovalAppliedEvidenceMatches(
		intent,
		state.Values[5],
	) || string(state.Values[6].Value) != task.ID {
		return nil, nil, errs.New(errs.KindStateConflict, "Zone removal terminal ownership changed")
	}
	if intent.Authored == nil {
		if state.Values[4] == nil || state.Values[4].ModRevision != intent.DesiredHeadRevision {
			return nil, nil, errs.New(errs.KindStateConflict, "Zone removal desired head changed")
		}
		headID, decodeErr := idempotencyrecord.DecodeTaskReference(state.Values[4].Value)
		if decodeErr != nil || headID != intent.DesiredProjection.RevisionID {
			return nil, nil, errs.New(errs.KindStateConflict, "Zone removal desired head changed")
		}
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
	pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](state.Values[1].Value, "zone_pool_registry")
	if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil ||
		pool.Reservations[task.Target] != zone.Desired.Subnet {
		return nil, nil, errs.New(errs.KindInternal, "Zone subnet reservation is inconsistent")
	}
	if state.Values[2] != nil {
		addresses, decodeErr := recordcodec.Decode[networkreservations.ComponentAddressRegistry](
			state.Values[2].Value,
			"component_address_registry",
		)
		if decodeErr != nil || networkreservations.ValidateComponentAddressRegistry(zone, addresses) != nil ||
			len(addresses.Reservations) != 0 {
			return nil, nil, errs.New(errs.KindResourceInUse, "Zone gained a Component address reservation")
		}
	}
	conditions := []etcdstore.Condition{
		{Key: keys[0], ModRevision: state.Values[0].ModRevision},
		{Key: keys[1], ModRevision: state.Values[1].ModRevision},
		{Key: keys[2], ModRevision: etcdstore.RevisionOf(state.Values[2])},
		{Key: keys[3], ModRevision: state.Values[3].ModRevision},
		{Key: keys[5], ModRevision: etcdstore.RevisionOf(state.Values[5])},
		{Key: keys[6], ModRevision: state.Values[6].ModRevision},
	}
	if intent.Authored == nil {
		conditions = append(conditions, etcdstore.Condition{Key: keys[4], ModRevision: state.Values[4].ModRevision})
	}
	if terminalStatus != taskjournal.TaskStatusCompleted {
		terminalIntent, transitionErr := environmentchanges.TerminalZoneRemovalIntent(
			intent,
			terminalStatus,
			terminalAt,
		)
		if transitionErr != nil {
			return nil, nil, transitionErr
		}
		intentValue, encodeErr := environmentchanges.EncodeZoneRemovalIntent(terminalIntent)
		if encodeErr != nil {
			return nil, nil, encodeErr
		}
		return conditions, []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: keys[3], Value: intentValue},
			{Type: etcdstore.MutationDelete, Key: keys[0]},
			{Type: etcdstore.MutationDelete, Key: keys[6]},
		}, nil
	}
	dnsConditions, withdrawn, err := repository.zoneDNSWithdrawalConditions(ctx, zone.Desired,
		readRevision, resolverWithdrawalFinalization)
	if err != nil {
		return nil, nil, err
	}
	if !withdrawn {
		return nil, nil, errs.New(
			errs.KindResourceInUse,
			"CoreDNS still owns the Zone gateway; subnet reservation is retained",
		)
	}
	conditions = append(conditions, dnsConditions...)
	if intent.Authored != nil {
		projectionValue, encodeErr := projectionrecord.EncodeEnvironmentComposeProjectionStorage(
			intent.CandidateProjection,
		)
		if encodeErr != nil {
			return nil, nil, encodeErr
		}
		nextPool, releaseErr := pool.Release(zone)
		if releaseErr != nil {
			clear(projectionValue)
			return nil, nil, releaseErr
		}
		mutations := []etcdstore.Mutation{
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
				etcdstore.ClearMutationValues(mutations)
				return nil, nil, encodeErr
			}
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[1], Value: poolValue})
		}
		return conditions, mutations, nil
	}
	hierarchy := composeHierarchyRepository(repository.store)
	publication, err := hierarchy.prepareEnvironmentDirectPublication(
		ctx,
		intent.Claim,
		blueprints.EnvironmentDesiredRevisionIdentity{
			EnvironmentID: intent.EnvironmentID,
			RevisionID:    intent.Claim.RevisionID,
		},
		intent.CandidateProjection,
		idempotencyrecord.IdempotencyMarker{
			Locator: intent.Claim.Locator,
			Intent:  intent.Claim.Intent,
		},
		intent.DesiredHeadRevision,
	)
	if err != nil {
		return nil, nil, err
	}
	identityPublication, err := prepareEnvironmentDesiredIdentityPublication(
		ctx,
		repository.store,
		intent.CandidateProjection,
		intent.Claim.SourceKind,
		intent.DesiredHeadRevision,
		readRevision,
	)
	if err != nil {
		clear(publication.publishedDescriptor)
		return nil, nil, err
	}
	headValue, err := idempotencyrecord.EncodeTaskReference(intent.Claim.RevisionID)
	if err != nil {
		clear(publication.publishedDescriptor)
		identityPublication.clear()
		return nil, nil, err
	}
	projectionValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(intent.CandidateProjection)
	if err != nil {
		clear(publication.publishedDescriptor)
		identityPublication.clear()
		clear(headValue)
		return nil, nil, err
	}
	nextPool, err := pool.Release(zone)
	if err != nil {
		clear(publication.publishedDescriptor)
		identityPublication.clear()
		clear(headValue)
		clear(projectionValue)
		return nil, nil, err
	}
	conditions = append(
		conditions,
		etcdstore.Condition{
			Key:         blueprints.EnvironmentBlueprintRootKey(intent.EnvironmentID, intent.Claim.RevisionID),
			ModRevision: publication.rootRevision,
		},
		etcdstore.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		etcdstore.Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
	)
	conditions, err = appendRemovalComparisons(conditions, identityPublication.conditions)
	if err != nil {
		clear(publication.publishedDescriptor)
		identityPublication.clear()
		clear(headValue)
		clear(projectionValue)
		return nil, nil, err
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: etcdstore.MutationDelete, Key: publication.locatorKey},
	}
	mutations = append(mutations, identityPublication.mutations...)
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[4], Value: headValue},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[3]},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[0]},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[6]},
	)
	if intent.AppliedProjectionRevision > 0 {
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[5], Value: projectionValue},
		)
	} else {
		clear(projectionValue)
	}
	if state.Values[2] != nil {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[2]})
	}
	if len(nextPool.Reservations) == 0 {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[1]})
	} else {
		poolValue, encodeErr := recordcodec.Encode("zone_pool_registry", nextPool)
		if encodeErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return nil, nil, encodeErr
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[1], Value: poolValue})
	}
	receipt, err := repository.prepareDirectRemovalReceipt(ctx, task, assignment, intent.EnvironmentID,
		blueprintunits.ResourceKey{Kind: ids.KindNetwork, ID: intent.ZoneID}, readRevision)
	if err != nil {
		etcdstore.ClearMutationValues(mutations)
		return nil, nil, err
	}
	conditions, err = appendRemovalComparisons(conditions, receipt.Conditions())
	if err != nil {
		receipt.Clear()
		etcdstore.ClearMutationValues(mutations)
		return nil, nil, err
	}
	mutations = append(mutations, receipt.Mutations()...)
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
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), task.Target),
		networkreservations.ZonePoolRegistryKey(environmentID), environmentchanges.ZoneRemovalIntentKey(operationID),
		blueprints.EnvironmentBlueprintHeadKey(
			environmentID,
		), projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(environmentID),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] != nil || state.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "Zone removal terminal state does not match its Task")
	}
	if authoredBlueprintZoneRemovalTask(task) {
		return validateAuthoredZoneRemovalReplay(task, terminalStatus, readRevision, state.Values)
	}
	if state.Values[3] == nil {
		return errs.New(errs.KindStateConflict, "Zone removal terminal state does not match its Task")
	}
	headID, err := idempotencyrecord.DecodeTaskReference(state.Values[3].Value)
	if err != nil {
		return err
	}
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if state.Values[2] != nil || headID != task.Params[blueprints.EnvironmentDesiredRevisionParam] {
			return errs.New(errs.KindStateConflict, "completed Zone removal retained old desired state")
		}
		desired, found, readErr := blueprints.ReadCurrentProjection(ctx, repository.store, environmentID, readRevision)
		if readErr != nil || !found || desired.Record.RevisionID != headID ||
			projectionContainsZone(desired.Record, task.Target) {
			return errs.New(errs.KindStateConflict, "completed Zone removal desired projection changed")
		}
		if state.Values[4] != nil {
			applied, decodeErr := projectionrecord.DecodeEnvironmentComposeProjectionStorage(state.Values[4].Value)
			if decodeErr != nil || applied.RevisionID != headID || projectionContainsZone(applied, task.Target) {
				return errs.New(errs.KindStateConflict, "completed Zone removal applied projection changed")
			}
		}
		if state.Values[1] != nil {
			pool, poolErr := recordcodec.Decode[networkreservations.ZonePoolRegistry](
				state.Values[1].Value,
				"zone_pool_registry",
			)
			if poolErr != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil {
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
	intent, err := environmentchanges.DecodeZoneRemovalIntent(state.Values[2].Value)
	if err != nil || validateZoneRemovalTaskOwner(task, intent) != nil || intent.Status != terminalStatus ||
		intent.TerminalAt == nil || task.FinishedAt == nil || !intent.TerminalAt.Equal(*task.FinishedAt) ||
		state.Values[3].ModRevision != intent.DesiredHeadRevision || headID != intent.DesiredProjection.RevisionID ||
		!environmentchanges.ZoneRemovalAppliedEvidenceMatches(intent, state.Values[4]) {
		return errs.New(errs.KindStateConflict, "failed Zone removal changed sealed desired state")
	}
	zone, err := projectedZoneRemovalTarget(intent, readRevision)
	if err != nil {
		return err
	}
	pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](state.Values[1].Value, "zone_pool_registry")
	if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil ||
		pool.Reservations[task.Target] != zone.Desired.Subnet {
		return errs.New(errs.KindStateConflict, "failed Zone removal changed its subnet reservation")
	}
	return nil
}

func validateAuthoredZoneRemovalReplay(
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	readRevision int64,
	values []*etcdstore.KeyValue,
) error {
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if values[2] != nil || values[4] == nil {
			return errs.New(errs.KindStateConflict, "completed authored Zone removal retained old state")
		}
		applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(values[4].Value)
		parentID := task.Params[taskjournal.TaskBlueprintParentParam]
		if err != nil || applied.EnvironmentID != task.Owner.EnvironmentID ||
			projectionContainsZone(applied, task.Target) ||
			applied.RenderGeneration < uint64(task.RenderGeneration) ||
			applied.RenderGeneration == uint64(task.RenderGeneration) && applied.RevisionID != parentID {
			return errs.New(errs.KindStateConflict, "completed authored Zone removal projection changed")
		}
		if values[1] != nil {
			pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](
				values[1].Value, "zone_pool_registry",
			)
			if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil {
				return errs.New(errs.KindInternal, "Zone pool registry is inconsistent")
			}
			if _, retained := pool.Reservations[task.Target]; retained {
				return errs.New(errs.KindStateConflict, "completed authored Zone removal retained its subnet")
			}
		}
		return nil
	}
	if values[1] == nil || values[2] == nil {
		return errs.New(errs.KindStateConflict, "failed authored Zone removal lost durable state")
	}
	intent, err := environmentchanges.DecodeZoneRemovalIntent(values[2].Value)
	if err != nil || intent.Authored == nil || validateZoneRemovalTaskOwner(task, intent) != nil ||
		intent.Status != terminalStatus || intent.TerminalAt == nil || task.FinishedAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) ||
		!environmentchanges.ZoneRemovalAppliedEvidenceMatches(intent, values[4]) {
		return errs.New(errs.KindStateConflict, "failed authored Zone removal changed sealed applied state")
	}
	zone, err := projectedZoneRemovalTarget(intent, readRevision)
	if err != nil {
		return err
	}
	pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](values[1].Value, "zone_pool_registry")
	if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil ||
		pool.Reservations[task.Target] != zone.Desired.Subnet {
		return errs.New(errs.KindStateConflict, "failed authored Zone removal changed its subnet reservation")
	}
	return nil
}

func projectedZoneRemovalTarget(
	intent environmentchanges.ZoneRemovalIntent,
	readRevision int64,
) (zonerecord.Record, error) {
	source := intent.DesiredProjection
	sourceRevision := intent.DesiredHeadRevision
	if intent.Authored != nil {
		source = intent.AppliedProjection
		sourceRevision = intent.AppliedProjectionRevision
	}
	projection := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record: source, Revision: sourceRevision, ReadRevision: readRevision,
	}
	var matched *zonerecord.Record
	for _, desired := range source.DesiredZones {
		if desired.Desired.ID != intent.ZoneID {
			continue
		}
		if matched != nil {
			return zonerecord.Record{}, errs.New(errs.KindInternal, "Zone removal projection has duplicate target")
		}
		joined, err := environmentqueries.JoinZone(projection, desired)
		if err != nil {
			return zonerecord.Record{}, err
		}
		matched = &joined.Record
	}
	if matched == nil || matched.EnvironmentID != intent.EnvironmentID || matched.Desired.Name != intent.ZoneName ||
		intent.ZoneRevision != sourceRevision {
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
	environmentID := task.Params[taskjournal.TaskZoneEnvironmentParam]
	operationID := task.Params[taskjournal.TaskZoneRemovalOperationParam]
	if task.Type != taskjournal.TaskRemove || ids.Validate(ids.KindNetwork, task.Target) != nil ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return "", "", errs.New(errs.KindInternal, "durable Zone removal Task shape is invalid")
	}
	return environmentID, operationID, nil
}
