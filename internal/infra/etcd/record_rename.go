package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func renameRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	current etcdstore.Versioned[T],
	replacement T,
	primaryKey string,
	oldSlugKey string,
	newSlugKey string,
	membershipKeys []string,
	tombstoneKey string,
	kind string,
	id string,
	notFound errs.Kind,
	encode func(T) ([]byte, error),
) (etcdstore.Versioned[T], error) {
	secondaryKeys := append([]string{oldSlugKey}, membershipKeys...)
	tombstoneOffset := len(secondaryKeys)
	secondaryKeys = append(secondaryKeys, tombstoneKey)
	newSlugOffset := -1
	if newSlugKey != oldSlugKey {
		newSlugOffset = len(secondaryKeys)
		secondaryKeys = append(secondaryKeys, newSlugKey)
	}
	secondary, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: secondaryKeys, Revision: current.ReadRevision})
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	if len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != id {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindInternal, "slug index is missing or mismatched")
	}
	for index := range membershipKeys {
		value := secondary.Values[index+1]
		if value == nil || string(value.Value) != id {
			return etcdstore.Versioned[T]{}, errs.New(errs.KindInternal, "owner index is missing or mismatched")
		}
	}
	if secondary.Values[tombstoneOffset] != nil {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindResourceInUse, "resource deletion is in progress")
	}
	if newSlugOffset >= 0 && secondary.Values[newSlugOffset] != nil {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindSlugConflict, "slug is already in use")
	}
	if newSlugOffset < 0 {
		if err := validateContext(ctx); err != nil {
			return etcdstore.Versioned[T]{}, err
		}
		return current, nil
	}
	value, err := encode(replacement)
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: primaryKey, ModRevision: current.Revision},
		{Key: oldSlugKey, ModRevision: secondary.Values[0].ModRevision},
	}
	for index, key := range membershipKeys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: secondary.Values[index+1].ModRevision})
	}
	conditions = append(conditions, etcdstore.Condition{Key: tombstoneKey})
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: primaryKey, Value: value}}
	conditions = append(conditions, etcdstore.Condition{Key: newSlugKey})
	mutations = append(mutations,
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: oldSlugKey},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: newSlugKey, Value: []byte(id)},
	)
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	result, err := store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[T]{}, diagnoseRename(
			ctx, store, primaryKey, current.Revision, oldSlugKey, newSlugKey,
			membershipKeys, tombstoneKey, kind, id, notFound,
		)
	}
	return etcdstore.Versioned[T]{Record: replacement, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func diagnoseRename(
	ctx context.Context,
	store hierarchyStore,
	primaryKey string,
	expectedRevision int64,
	oldSlugKey string,
	newSlugKey string,
	membershipKeys []string,
	tombstoneKey string,
	kind string,
	id string,
	notFound errs.Kind,
) error {
	keys := []string{primaryKey, oldSlugKey}
	membershipOffset := len(keys)
	keys = append(keys, membershipKeys...)
	tombstoneOffset := len(keys)
	keys = append(keys, tombstoneKey)
	newSlugOffset := len(keys)
	keys = append(keys, newSlugKey)
	current, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if len(current.Values) != len(keys) {
		return errs.New(errs.KindInternal, "rename diagnosis returned an invalid key count")
	}
	if current.Values[0] == nil {
		return errs.Newf(notFound, "%s was not found", id)
	}
	if current.Values[tombstoneOffset] != nil {
		return errs.New(errs.KindResourceInUse, "resource deletion is in progress")
	}
	if current.Values[newSlugOffset] != nil && string(current.Values[newSlugOffset].Value) != id {
		return errs.New(errs.KindSlugConflict, "slug is already in use")
	}
	if current.Values[0].ModRevision != expectedRevision {
		return stateConflict(kind, id)
	}
	if current.Values[1] == nil || string(current.Values[1].Value) != id {
		return errs.New(errs.KindInternal, "slug index is missing or mismatched")
	}
	for index := range membershipKeys {
		value := current.Values[membershipOffset+index]
		if value == nil || string(value.Value) != id {
			return errs.New(errs.KindInternal, "owner index is missing or mismatched")
		}
	}
	return stateConflict(kind, id)
}

func stateConflict(kind string, id string) error {
	return errs.Newf(errs.KindStateConflict, "%s %s changed", kind, id)
}
