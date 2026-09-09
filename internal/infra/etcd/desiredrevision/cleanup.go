package desiredrevision

import (
	"context"
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
	claim etcd.EnvironmentBlueprintStageClaim,
) error {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return err
	}
	if err := etcd.ValidateDesiredRevisionClaim(claim); err != nil {
		return err
	}
	descriptorKey := etcd.DesiredRevisionDescriptorKey(claim.DescriptorID)
	locatorKey, _, err := etcd.DesiredRevisionLocatorKey(claim.Locator)
	if err != nil {
		return err
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(claim.Locator)
	if err != nil {
		return err
	}
	evidence, err := repository.store.GetMany(
		ctx,
		etcd.GetManyRequest{Keys: []string{descriptorKey, locatorKey, markerKey, etcd.CapabilityTaskKey(claim.TaskID)}},
	)
	if err != nil {
		return err
	}
	if evidence == nil || len(evidence.Values) != 4 || evidence.Values[0] == nil {
		return etcd.CorruptDesiredRevisionStage()
	}
	defer clearKeyValues(evidence.Values)
	descriptor, err := etcd.DecodeDesiredRevisionDescriptor(evidence.Values[0].Value)
	if err != nil || !etcd.SameDesiredRevisionClaim(descriptor.Claim, claim) {
		return etcd.CorruptDesiredRevisionStage()
	}
	if descriptor.State == etcd.EnvironmentBlueprintStageAbandoned {
		return nil
	}
	if descriptor.State == etcd.EnvironmentBlueprintStagePublished || evidence.Values[1] == nil ||
		evidence.Values[2] != nil || evidence.Values[3] != nil {
		return errs.New(errs.KindStateConflict, "Blueprint staging claim cannot be abandoned")
	}
	owned, err := environmentBlueprintLocatorOwnedBy(evidence.Values[1].Value, descriptor)
	if err != nil || !owned {
		return etcd.CorruptDesiredRevisionStage()
	}
	abandoned := descriptor
	abandoned.State = etcd.EnvironmentBlueprintStageAbandoned
	abandoned.UpdatedAt = etcd.NextDesiredRevisionProgressTime(descriptor.UpdatedAt)
	value, err := etcd.EncodeDesiredRevisionDescriptor(abandoned)
	if err != nil {
		return err
	}
	defer clear(value)
	conditions := []etcd.Condition{
		{Key: descriptorKey, ModRevision: evidence.Values[0].ModRevision},
		{Key: locatorKey, ModRevision: evidence.Values[1].ModRevision},
		{Key: markerKey},
	}
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: descriptorKey, Value: value},
		{Type: etcd.MutationDelete, Key: locatorKey},
	}
	if err := etcd.ValidateDesiredRevisionTransaction(repository.store, conditions, mutations, 5, etcd.EnvironmentBlueprintGCTransactionBytes); err != nil {
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
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return 0, err
	}
	if !etcd.ValidDesiredRevisionTime(now) || limit < 1 || limit > 128 {
		return 0, errs.New(errs.KindValidationFailed, "Blueprint staging cleanup request is invalid")
	}
	page, err := repository.store.Range(ctx, etcd.RangeRequest{
		Prefix: etcd.EnvironmentBlueprintDescriptorPrefix,
		Limit:  int64(limit),
	})
	if err != nil {
		return 0, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > limit {
		return 0, etcd.CorruptDesiredRevisionStage()
	}
	defer clearKeyValueSlice(page.Values)
	processed := 0
	for index := range page.Values {
		entry := page.Values[index]
		descriptorID, err := parseEnvironmentBlueprintDescriptorKey(entry.Key)
		if err != nil {
			return processed, err
		}
		descriptor, err := etcd.DecodeDesiredRevisionDescriptor(entry.Value)
		if err != nil || descriptor.Claim.DescriptorID != descriptorID {
			return processed, etcd.CorruptDesiredRevisionStage()
		}
		switch descriptor.State {
		case etcd.EnvironmentBlueprintStageOpen, etcd.EnvironmentBlueprintStageSealed:
			if descriptor.UpdatedAt.After(now.Add(-etcd.EnvironmentBlueprintStageExpiry)) {
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
			descriptor.State = etcd.EnvironmentBlueprintStageAbandoned
		case etcd.EnvironmentBlueprintStageAbandoned:
		case etcd.EnvironmentBlueprintStagePublished:
			deleted, cleanupErr := repository.cleanupPublishedEnvironmentBlueprintDescriptor(ctx, descriptor)
			if cleanupErr != nil {
				return processed, cleanupErr
			}
			if deleted {
				processed++
			}
			continue
		default:
			return processed, etcd.CorruptDesiredRevisionStage()
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
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
	descriptorRevision int64,
	revision int64,
	now time.Time,
) (bool, error) {
	descriptorKey := etcd.DesiredRevisionDescriptorKey(descriptor.Claim.DescriptorID)
	locatorKey, _, err := etcd.DesiredRevisionLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	evidence, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
		Keys: []string{
			descriptorKey,
			locatorKey,
			markerKey,
			etcd.CapabilityTaskKey(descriptor.Claim.TaskID),
		}, Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if evidence == nil || len(evidence.Values) != 4 || evidence.Values[0] == nil || evidence.Values[1] == nil {
		return false, etcd.CorruptDesiredRevisionStage()
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
		return false, etcd.CorruptDesiredRevisionStage()
	}
	abandoned := descriptor
	abandoned.State = etcd.EnvironmentBlueprintStageAbandoned
	abandoned.UpdatedAt = now
	if !abandoned.UpdatedAt.After(descriptor.UpdatedAt) {
		abandoned.UpdatedAt = descriptor.UpdatedAt.Add(time.Nanosecond)
	}
	value, err := etcd.EncodeDesiredRevisionDescriptor(abandoned)
	if err != nil {
		return false, err
	}
	defer clear(value)
	conditions := []etcd.Condition{
		{Key: descriptorKey, ModRevision: descriptorRevision},
		{Key: locatorKey, ModRevision: evidence.Values[1].ModRevision},
		{Key: markerKey},
	}
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: descriptorKey, Value: value},
		{Type: etcd.MutationDelete, Key: locatorKey},
	}
	if err := etcd.ValidateDesiredRevisionTransaction(repository.store, conditions, mutations, 5, etcd.EnvironmentBlueprintGCTransactionBytes); err != nil {
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
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
	now time.Time,
) (bool, error) {
	chunkPrefix := etcd.DesiredRevisionPrefix(
		descriptor.Claim.EnvironmentID,
		descriptor.Claim.RevisionID,
	) + "chunks/"
	page, err := repository.store.Range(ctx, etcd.RangeRequest{Prefix: chunkPrefix, Limit: 33})
	if err != nil {
		return false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 33 {
		return false, etcd.CorruptDesiredRevisionStage()
	}
	defer clearKeyValueSlice(page.Values)
	descriptorKey := etcd.DesiredRevisionDescriptorKey(descriptor.Claim.DescriptorID)
	locatorKey, _, err := etcd.DesiredRevisionLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	evidence, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
		Keys: []string{descriptorKey, locatorKey, markerKey}, Revision: page.ReadRevision,
	})
	if err != nil {
		return false, err
	}
	if evidence == nil || len(evidence.Values) != 3 || evidence.Values[0] == nil {
		return false, etcd.CorruptDesiredRevisionStage()
	}
	defer clearKeyValues(evidence.Values)
	stored, err := etcd.DecodeDesiredRevisionDescriptor(evidence.Values[0].Value)
	if err != nil || stored.State != etcd.EnvironmentBlueprintStageAbandoned ||
		!etcd.SameDesiredRevisionClaim(stored.Claim, descriptor.Claim) {
		return false, etcd.CorruptDesiredRevisionStage()
	}
	if evidence.Values[1] != nil {
		owned, locatorErr := environmentBlueprintLocatorOwnedBy(evidence.Values[1].Value, stored)
		if locatorErr != nil || owned {
			return false, etcd.CorruptDesiredRevisionStage()
		}
	}
	if evidence.Values[2] != nil {
		owned, markerErr := environmentBlueprintMarkerOwnedBy(evidence.Values[2].Value, stored)
		if markerErr != nil || owned {
			return false, etcd.CorruptDesiredRevisionStage()
		}
	}
	count := len(page.Values)
	if count > 32 {
		count = 32
	}
	final := count == len(page.Values) && !page.More
	conditions := make([]etcd.Condition, 0, count+1)
	mutations := make([]etcd.Mutation, 0, count+1)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if entry.ModRevision <= 0 || !strings.HasPrefix(entry.Key, chunkPrefix) {
			return false, etcd.CorruptDesiredRevisionStage()
		}
		conditions = append(conditions, etcd.Condition{Key: entry.Key, ModRevision: entry.ModRevision})
		mutations = append(mutations, etcd.Mutation{Type: etcd.MutationDelete, Key: entry.Key})
	}
	conditions = append(conditions, etcd.Condition{Key: descriptorKey, ModRevision: evidence.Values[0].ModRevision})
	if final {
		mutations = append(mutations, etcd.Mutation{Type: etcd.MutationDelete, Key: descriptorKey})
	} else {
		stored.UpdatedAt = now
		if !stored.UpdatedAt.After(descriptor.UpdatedAt) {
			stored.UpdatedAt = descriptor.UpdatedAt.Add(time.Nanosecond)
		}
		value, encodeErr := etcd.EncodeDesiredRevisionDescriptor(stored)
		if encodeErr != nil {
			return false, encodeErr
		}
		defer clear(value)
		mutations = append(mutations, etcd.Mutation{Type: etcd.MutationPut, Key: descriptorKey, Value: value})
	}
	if err := etcd.ValidateDesiredRevisionTransaction(repository.store, conditions, mutations, 66, etcd.EnvironmentBlueprintGCTransactionBytes); err != nil {
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
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
) (bool, error) {
	descriptorKey := etcd.DesiredRevisionDescriptorKey(descriptor.Claim.DescriptorID)
	locatorKey, _, err := etcd.DesiredRevisionLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	markerKey, err := etcd.CapabilityIdempotencyMarkerKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	rootKey := etcd.DesiredRevisionRootKey(descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID)
	taskPrimaryKey := etcd.CapabilityTaskKey(descriptor.Claim.TaskID)
	evidence, err := repository.store.GetMany(ctx, etcd.GetManyRequest{
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
	stored, err := etcd.DecodeDesiredRevisionDescriptor(evidence.Values[0].Value)
	if err != nil || stored.State != etcd.EnvironmentBlueprintStagePublished ||
		!etcd.SameDesiredRevisionClaim(stored.Claim, descriptor.Claim) {
		return false, etcd.CorruptDesiredRevisionStage()
	}
	if evidence.Values[1] != nil {
		owned, locatorErr := environmentBlueprintLocatorOwnedBy(evidence.Values[1].Value, stored)
		if locatorErr != nil || owned {
			return false, etcd.CorruptDesiredRevisionStage()
		}
	}
	markerOwned, err := environmentBlueprintMarkerOwnedBy(evidence.Values[2].Value, stored)
	if err != nil || !markerOwned {
		return false, etcd.CorruptDesiredRevisionStage()
	}
	root, err := etcd.DecodeDesiredRevisionSeal(evidence.Values[3].Value)
	if err != nil || root != etcd.DesiredRevisionSealFromDescriptor(stored) {
		return false, etcd.CorruptDesiredRevisionStage()
	}
	task, err := etcd.DecodeCapabilityTaskRecord(evidence.Values[4].Value)
	if err != nil || task.ID != stored.Claim.TaskID ||
		task.Params[etcd.EnvironmentDesiredRevisionParam] != stored.Claim.RevisionID {
		return false, etcd.CorruptDesiredRevisionStage()
	}
	conditions := []etcd.Condition{{Key: descriptorKey, ModRevision: evidence.Values[0].ModRevision}}
	mutations := []etcd.Mutation{{Type: etcd.MutationDelete, Key: descriptorKey}}
	if err := etcd.ValidateDesiredRevisionTransaction(repository.store, conditions, mutations, 2, etcd.EnvironmentBlueprintGCTransactionBytes); err != nil {
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
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
) (bool, error) {
	descriptorID, digest, err := etcd.DecodeDesiredRevisionLocator(value)
	if err != nil {
		return false, err
	}
	want, err := etcd.ProtectedDesiredRevisionIntentDigest(descriptor.Claim.Intent)
	if err != nil {
		return false, err
	}
	return descriptorID == descriptor.Claim.DescriptorID && digest == want, nil
}

func environmentBlueprintMarkerOwnedBy(
	value []byte,
	descriptor etcd.EnvironmentBlueprintStageDescriptor,
) (bool, error) {
	marker, err := etcd.DecodeCapabilityIdempotencyMarker(value, descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	return marker.TaskID == descriptor.Claim.TaskID &&
		marker.Intent.CiphertextDigest == descriptor.Claim.Intent.CiphertextDigest, nil
}

func parseEnvironmentBlueprintDescriptorKey(key string) (string, error) {
	segment := strings.TrimPrefix(key, etcd.EnvironmentBlueprintDescriptorPrefix)
	if segment == key || strings.Contains(segment, "/") {
		return "", etcd.CorruptDesiredRevisionStage()
	}
	decoded, err := etcd.DecodeDesiredRevisionKeySegment(segment)
	if err != nil || !utf8.Valid(decoded) {
		return "", etcd.CorruptDesiredRevisionStage()
	}
	descriptorID := string(decoded)
	if ids.Validate(ids.KindTask, "task_"+descriptorID) != nil ||
		etcd.EnvironmentBlueprintDescriptorPrefix+etcd.EncodeCapabilityKeySegment(descriptorID) != key {
		return "", etcd.CorruptDesiredRevisionStage()
	}
	return descriptorID, nil
}
