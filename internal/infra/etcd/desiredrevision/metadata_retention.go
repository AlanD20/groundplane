package desiredrevision

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintretention"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Synchronous metadata publication has no Task. Its descriptor owns cleanup
// after the replay window, using the same live-reference guards as Task-owned
// revisions. Keep the immutable root until every companion record is gone.
func (repository *Repository) cleanupPublishedMetadataRevision(
	ctx context.Context, descriptor blueprints.EnvironmentBlueprintStageDescriptor, now time.Time,
) (bool, error) {
	if descriptor.State != blueprints.EnvironmentBlueprintStagePublishedMetadata ||
		descriptor.Claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		descriptor.Claim.TaskID != descriptor.Claim.RevisionID {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if now.Before(descriptor.Claim.CreatedAt.Add(idempotency.MarkerRetention)) {
		return false, nil
	}
	prefix := blueprints.EnvironmentBlueprintRevisionPrefixFinal(
		descriptor.Claim.EnvironmentID,
		descriptor.Claim.RevisionID,
	)
	page, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix, Limit: 33})
	if err != nil {
		return false, err
	}
	if page == nil || page.ReadRevision <= 0 || len(page.Values) > 33 || page.More && len(page.Values) == 0 {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer keyvalue.ClearRangeValues(page.Values)
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID)
	markerKey, err := idempotency.IdempotencyMarkerKey(descriptor.Claim.Locator)
	if err != nil {
		return false, err
	}
	rootKey := blueprints.EnvironmentBlueprintRootKey(descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID)
	taskKey := taskjournal.TaskStorageKey(descriptor.Claim.TaskID)
	keys := []string{descriptorKey, markerKey, rootKey, taskKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: page.ReadRevision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != page.ReadRevision || len(read.Values) != len(keys) || read.Values[0] == nil {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	defer keyvalue.ClearValues(read.Values)
	stored, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(read.Values[0].Value)
	if err != nil || stored.State != descriptor.State ||
		!blueprints.SameEnvironmentBlueprintStageClaim(stored.Claim, descriptor.Claim) {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	if read.Values[3] != nil {
		return false, errs.New(errs.KindInternal, "metadata revision unexpectedly has a Task owner")
	}
	if read.Values[1] != nil {
		marker, err := idempotency.DecodeIdempotencyMarker(read.Values[1].Value, stored.Claim.Locator)
		if err != nil {
			return false, err
		}
		owned := blueprints.SameBlueprintProtectedIntent(marker.Intent, stored.Claim.Intent)
		clear(marker.Intent.Ciphertext)
		clear(marker.Response.Body)
		if owned {
			return false, nil
		}
	}
	conditions, held, err := blueprintretention.Guards(ctx, repository.store,
		stored.Claim.EnvironmentID, stored.Claim.RevisionID, page.ReadRevision)
	if err != nil || held {
		return false, err
	}
	for index, key := range keys {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	if read.Values[2] == nil {
		if len(page.Values) != 0 {
			return false, errs.New(errs.KindInternal, "metadata revision companions have no immutable root")
		}
		conditions = append(conditions, keyvalue.Condition{Key: prefix, Prefix: true})
		return repository.retireMetadataBatch(ctx, conditions,
			[]keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: descriptorKey}}, true)
	}
	root, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[2].Value)
	if err != nil || root != blueprints.EnvironmentBlueprintSealFromDescriptor(stored) {
		return false, blueprints.CorruptEnvironmentBlueprintStage()
	}
	mutations := make([]keyvalue.Mutation, 0, 32)
	final := len(page.Values) == 1 && page.Values[0].Key == rootKey && !page.More
	for _, value := range page.Values {
		if value.ModRevision <= 0 || !strings.HasPrefix(value.Key, prefix) {
			return false, blueprints.CorruptEnvironmentBlueprintStage()
		}
		if value.Key == rootKey {
			if !final {
				continue
			}
		} else {
			conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: value.Key})
		if len(mutations) == 32 {
			break
		}
	}
	if final {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: descriptorKey})
	}
	if len(mutations) == 0 {
		return false, errs.New(errs.KindInternal, "metadata revision cleanup made no progress")
	}
	return repository.retireMetadataBatch(ctx, conditions, mutations, final)
}

func (repository *Repository) retireMetadataBatch(
	ctx context.Context, conditions []keyvalue.Condition, mutations []keyvalue.Mutation, final bool,
) (bool, error) {
	if err := etcd.ValidateBlueprintTransaction(repository.store, conditions, mutations, 128,
		blueprints.EnvironmentBlueprintGCTransactionBytes); err != nil {
		return false, err
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	keyvalue.ClearValues(result.FailureReads)
	return result.Succeeded && final, nil
}
