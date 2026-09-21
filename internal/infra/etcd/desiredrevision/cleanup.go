package desiredrevision

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AbandonEnvironmentBlueprintStage releases a locator after a known head or
// dependency conflict. It never treats marker absence as an unknown
// publication outcome; callers may use it only after they classified a known
// compare failure.
func (repository *Repository) AbandonEnvironmentBlueprintStage(
	ctx context.Context,
	claim blueprints.EnvironmentBlueprintStageClaim,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := blueprints.ValidateEnvironmentBlueprintStageClaim(claim); err != nil {
		return err
	}
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID)
	locatorKey, _, err := blueprints.EnvironmentBlueprintLocatorKey(claim.Locator)
	if err != nil {
		return err
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(claim.Locator)
	if err != nil {
		return err
	}
	evidence, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{
			Keys: []string{descriptorKey, locatorKey, markerKey, taskjournal.TaskStorageKey(claim.TaskID)},
		},
	)
	if err != nil {
		return err
	}
	if evidence == nil || len(evidence.Values) != 4 || evidence.Values[0] == nil {
		return blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clearKeyValues(evidence.Values)
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(evidence.Values[0].Value)
	if err != nil || !blueprints.SameEnvironmentBlueprintStageClaim(descriptor.Claim, claim) {
		return blueprints.CorruptEnvironmentBlueprintStage()
	}
	if descriptor.State == blueprints.EnvironmentBlueprintStageAbandoned {
		return nil
	}
	if descriptor.State == blueprints.EnvironmentBlueprintStagePublished || evidence.Values[1] == nil ||
		evidence.Values[2] != nil || evidence.Values[3] != nil {
		return errs.New(errs.KindStateConflict, "Blueprint staging claim cannot be abandoned")
	}
	owned, err := environmentBlueprintLocatorOwnedBy(evidence.Values[1].Value, descriptor)
	if err != nil || !owned {
		return blueprints.CorruptEnvironmentBlueprintStage()
	}
	abandoned := descriptor
	abandoned.State = blueprints.EnvironmentBlueprintStageAbandoned
	abandoned.UpdatedAt = blueprints.NextBlueprintProgressTime(descriptor.UpdatedAt)
	value, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(abandoned)
	if err != nil {
		return err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: descriptorKey, ModRevision: evidence.Values[0].ModRevision},
		{Key: locatorKey, ModRevision: evidence.Values[1].ModRevision},
		{Key: markerKey},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: descriptorKey, Value: value},
		{Type: etcdstore.MutationDelete, Key: locatorKey},
	}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 5, blueprints.EnvironmentBlueprintGCTransactionBytes); err != nil {
		return err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint staging abandonment evidence changed")
	}
	return nil
}

// CleanupEnvironmentBlueprintStaging performs bounded leader-owned private
// staging cleanup. Each invocation processes at most limit descriptors and at
// most one 32-record chunk batch per descriptor.
func (repository *Repository) CleanupEnvironmentBlueprintStaging(
	ctx context.Context,
	now time.Time,
	limit int,
) (int, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if !blueprints.ValidBlueprintRecordTime(now) || limit < 1 || limit > 128 {
		return 0, errs.New(errs.KindValidationFailed, "Blueprint staging cleanup request is invalid")
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: blueprints.EnvironmentBlueprintDescriptorPrefix,
		Limit:  int64(limit),
	})
	if err != nil {
		return 0, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > limit {
		return 0, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clearKeyValueSlice(page.Values)
	processed := 0
	for index := range page.Values {
		entry := page.Values[index]
		descriptorID, err := parseEnvironmentBlueprintDescriptorKey(entry.Key)
		if err != nil {
			return processed, err
		}
		descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(entry.Value)
		if err != nil || descriptor.Claim.DescriptorID != descriptorID {
			return processed, blueprints.CorruptEnvironmentBlueprintStage()
		}
		switch descriptor.State {
		case blueprints.EnvironmentBlueprintStageOpen, blueprints.EnvironmentBlueprintStageSealed:
			if descriptor.UpdatedAt.After(now.Add(-blueprints.EnvironmentBlueprintStageExpiry)) {
				continue
			}
			transitioned, transitionErr := repository.expireEnvironmentBlueprintStage(
				ctx,
				descriptor,
				entry.ModRevision,
				page.ReadRevision,
				now,
			)
			if transitionErr != nil {
				return processed, transitionErr
			}
			if !transitioned {
				continue
			}
			descriptor.State = blueprints.EnvironmentBlueprintStageAbandoned
		case blueprints.EnvironmentBlueprintStageAbandoned:
		case blueprints.EnvironmentBlueprintStagePublished:
			deleted, cleanupErr := repository.cleanupPublishedEnvironmentBlueprintDescriptor(ctx, descriptor)
			if cleanupErr != nil {
				return processed, cleanupErr
			}
			if deleted {
				processed++
			}
			continue
		default:
			return processed, blueprints.CorruptEnvironmentBlueprintStage()
		}
		deleted, cleanupErr := repository.cleanupAbandonedEnvironmentBlueprintDescriptor(ctx, descriptor, now)
		if cleanupErr != nil {
			return processed, cleanupErr
		}
		if deleted {
			processed++
		}
	}
	return processed, nil
}

func (repository *Repository) expireEnvironmentBlueprintStage(
	ctx context.Context,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
	descriptorRevision int64,
	revision int64,
	now time.Time,
) (bool, error) {
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID)
	locatorKey, _, err := blueprints.EnvironmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			descriptorKey,
			locatorKey,
			markerKey,
			taskjournal.TaskStorageKey(descriptor.Claim.TaskID),
		}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if evidence == nil || len(evidence.Values) != 4 || evidence.Values[0] == nil || evidence.Values[1] == nil {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clearKeyValues(evidence.Values)
	// A deferred Entry cleanup keeps its candidate private while its durable
	// Task exists, even after the root response expires. Task publication also
	// advances the descriptor, so the existing CAS closes a late publication.
	if evidence.Values[0].ModRevision != descriptorRevision || evidence.Values[2] != nil || evidence.Values[3] != nil {
		return false, nil
	}
	owned, err := environmentBlueprintLocatorOwnedBy(evidence.Values[1].Value, descriptor)
	if err != nil || !owned {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	abandoned := descriptor
	abandoned.State = blueprints.EnvironmentBlueprintStageAbandoned
	abandoned.UpdatedAt = now
	if !abandoned.UpdatedAt.After(descriptor.UpdatedAt) {
		abandoned.UpdatedAt = descriptor.UpdatedAt.Add(time.Nanosecond)
	}
	value, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(abandoned)
	if err != nil {
		return false, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: descriptorKey, ModRevision: descriptorRevision},
		{Key: locatorKey, ModRevision: evidence.Values[1].ModRevision},
		{Key: markerKey},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: descriptorKey, Value: value},
		{Type: etcdstore.MutationDelete, Key: locatorKey},
	}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 5, blueprints.EnvironmentBlueprintGCTransactionBytes); err != nil {
		return false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	return result.Succeeded, nil
}

func (repository *Repository) cleanupAbandonedEnvironmentBlueprintDescriptor(
	ctx context.Context,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
	now time.Time,
) (bool, error) {
	chunkPrefix := blueprints.EnvironmentBlueprintRevisionPrefixFinal(
		descriptor.Claim.EnvironmentID,
		descriptor.Claim.RevisionID,
	) + "chunks/"
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: chunkPrefix, Limit: 33})
	if err != nil {
		return false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 33 {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clearKeyValueSlice(page.Values)
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID)
	locatorKey, _, err := blueprints.EnvironmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{descriptorKey, locatorKey, markerKey}, Revision: page.ReadRevision,
	})
	if err != nil {
		return false, err
	}
	if evidence == nil || len(evidence.Values) != 3 || evidence.Values[0] == nil {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer clearKeyValues(evidence.Values)
	stored, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(evidence.Values[0].Value)
	if err != nil || stored.State != blueprints.EnvironmentBlueprintStageAbandoned ||
		!blueprints.SameEnvironmentBlueprintStageClaim(stored.Claim, descriptor.Claim) {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if evidence.Values[1] != nil {
		owned, locatorErr := environmentBlueprintLocatorOwnedBy(evidence.Values[1].Value, stored)
		if locatorErr != nil || owned {
			return false, blueprints.CorruptEnvironmentBlueprintStage()
		}
	}
	if evidence.Values[2] != nil {
		owned, markerErr := environmentBlueprintMarkerOwnedBy(evidence.Values[2].Value, stored)
		if markerErr != nil || owned {
			return false, blueprints.CorruptEnvironmentBlueprintStage()
		}
	}
	count := len(page.Values)
	if count > 32 {
		count = 32
	}
	final := count == len(page.Values) && !page.More
	conditions := make([]etcdstore.Condition, 0, count+1)
	mutations := make([]etcdstore.Mutation, 0, count+1)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if entry.ModRevision <= 0 || !strings.HasPrefix(entry.Key, chunkPrefix) {
			return false, blueprints.CorruptEnvironmentBlueprintStage()
		}
		conditions = append(conditions, etcdstore.Condition{Key: entry.Key, ModRevision: entry.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entry.Key})
	}
	conditions = append(
		conditions,
		etcdstore.Condition{Key: descriptorKey, ModRevision: evidence.Values[0].ModRevision},
	)
	if final {
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: descriptorKey})
	} else {
		stored.UpdatedAt = now
		if !stored.UpdatedAt.After(descriptor.UpdatedAt) {
			stored.UpdatedAt = descriptor.UpdatedAt.Add(time.Nanosecond)
		}
		value, encodeErr := blueprints.EncodeEnvironmentBlueprintStageDescriptor(stored)
		if encodeErr != nil {
			return false, encodeErr
		}
		defer clear(value)
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: descriptorKey, Value: value})
	}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 66, blueprints.EnvironmentBlueprintGCTransactionBytes); err != nil {
		return false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	return result.Succeeded && final, nil
}

func (repository *Repository) cleanupPublishedEnvironmentBlueprintDescriptor(
	ctx context.Context,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
) (bool, error) {
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID)
	locatorKey, _, err := blueprints.EnvironmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	rootKey := blueprints.EnvironmentBlueprintRootKey(descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID)
	taskPrimaryKey := taskjournal.TaskStorageKey(descriptor.Claim.TaskID)
	evidence, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{descriptorKey, locatorKey, markerKey, rootKey, taskPrimaryKey},
	})
	if err != nil {
		return false, err
	}
	if evidence == nil || len(evidence.Values) != 5 || evidence.Values[0] == nil ||
		evidence.Values[2] == nil || evidence.Values[3] == nil || evidence.Values[4] == nil {
		return false, nil
	}
	defer clearKeyValues(evidence.Values)
	stored, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(evidence.Values[0].Value)
	if err != nil || stored.State != blueprints.EnvironmentBlueprintStagePublished ||
		!blueprints.SameEnvironmentBlueprintStageClaim(stored.Claim, descriptor.Claim) {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if evidence.Values[1] != nil {
		owned, locatorErr := environmentBlueprintLocatorOwnedBy(evidence.Values[1].Value, stored)
		if locatorErr != nil || owned {
			return false, blueprints.CorruptEnvironmentBlueprintStage()
		}
	}
	markerOwned, err := environmentBlueprintMarkerOwnedBy(evidence.Values[2].Value, stored)
	if err != nil || !markerOwned {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	root, err := blueprints.DecodeEnvironmentBlueprintSeal(evidence.Values[3].Value)
	if err != nil || root != blueprints.EnvironmentBlueprintSealFromDescriptor(stored) {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	task, err := etcd.DecodeTaskRecord(evidence.Values[4].Value)
	if err != nil || task.ID != stored.Claim.TaskID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != stored.Claim.RevisionID {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	conditions := []etcdstore.Condition{{Key: descriptorKey, ModRevision: evidence.Values[0].ModRevision}}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: descriptorKey}}
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 2, blueprints.EnvironmentBlueprintGCTransactionBytes); err != nil {
		return false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	return result.Succeeded, nil
}

func environmentBlueprintLocatorOwnedBy(
	value []byte,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
) (bool, error) {
	descriptorID, digest, err := blueprints.DecodeEnvironmentBlueprintStageLocator(value)
	if err != nil {
		return false, err
	}
	want, err := blueprints.ProtectedBlueprintIntentDigest(descriptor.Claim.Intent)
	if err != nil {
		return false, err
	}
	return descriptorID == descriptor.Claim.DescriptorID && digest == want, nil
}

func environmentBlueprintMarkerOwnedBy(
	value []byte,
	descriptor blueprints.EnvironmentBlueprintStageDescriptor,
) (bool, error) {
	marker, err := idempotencyrecord.DecodeIdempotencyMarker(value, descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	return marker.TaskID == descriptor.Claim.TaskID &&
		marker.Intent.CiphertextDigest == descriptor.Claim.Intent.CiphertextDigest, nil
}

func parseEnvironmentBlueprintDescriptorKey(key string) (string, error) {
	segment := strings.TrimPrefix(key, blueprints.EnvironmentBlueprintDescriptorPrefix)
	if segment == key || strings.Contains(segment, "/") {
		return "", blueprints.CorruptEnvironmentBlueprintStage()
	}
	decoded, err := blueprints.DecodeBlueprintDynamicBytes(segment)
	if err != nil || !utf8.Valid(decoded) {
		return "", blueprints.CorruptEnvironmentBlueprintStage()
	}
	descriptorID := string(decoded)
	if ids.Validate(ids.KindTask, "task_"+descriptorID) != nil ||
		blueprints.EnvironmentBlueprintDescriptorPrefix+recordcodec.EncodeKeySegment(descriptorID) != key {
		return "", blueprints.CorruptEnvironmentBlueprintStage()
	}
	return descriptorID, nil
}
