package desiredauthoring

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Cleanup removes only expired private metadata candidates. Changing the
// descriptor before deleting content fences a late publisher, including when
// cleanup needs several bounded transactions.
func Cleanup(ctx context.Context, store Store, descriptor blueprints.EnvironmentBlueprintStageDescriptor,
	revision int64, now time.Time,
) (bool, error) {
	if descriptor.State != blueprints.EnvironmentBlueprintStageMetadata ||
		descriptor.UpdatedAt.After(now.Add(-blueprints.EnvironmentBlueprintStageExpiry)) {
		return false, nil
	}
	page, err := store.Range(ctx, keyvalue.RangeRequest{
		Prefix: blueprints.EnvironmentBlueprintRevisionPrefixFinal(descriptor.Claim.EnvironmentID, descriptor.Claim.RevisionID), Limit: 32,
	})
	if err != nil {
		return false, err
	}
	if page == nil || len(page.Values) > 32 {
		return false, errs.New(errs.KindInternal, "metadata staging cleanup read is incomplete")
	}
	defer func() {
		for _, value := range page.Values {
			clear(value.Value)
		}
	}()
	key := blueprints.EnvironmentBlueprintDescriptorKeyByID(descriptor.Claim.DescriptorID)
	conditions := []keyvalue.Condition{{Key: key, ModRevision: revision}}
	mutations := make([]keyvalue.Mutation, 0, len(page.Values)+1)
	for _, value := range page.Values {
		conditions = append(conditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: value.Key})
	}
	if page.More {
		// Preserve expiry, but invalidate every prepared publication comparison.
		value, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(descriptor)
		if err != nil {
			return false, err
		}
		defer clear(value)
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	} else {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	result, err := store.Transact(ctx, conditions, mutations)
	return result.Succeeded && !page.More, err
}
