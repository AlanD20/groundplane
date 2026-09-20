package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func getRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	key string,
	id string,
	notFound errs.Kind,
	decode func([]byte) (T, error),
	identity func(T) string,
) (etcdstore.Versioned[T], error) {
	result, err := store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[T]{}, errs.Newf(notFound, "%s was not found", id)
	}
	record, err := decode(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	if identity(record) != id {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindInternal, "primary key does not match its record id")
	}
	return etcdstore.Versioned[T]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func resolveRecord[T any](
	ctx context.Context,
	store hierarchyStore,
	indexKey string,
	primaryKey func(string) string,
	idKind ids.Kind,
	notFound errs.Kind,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
) (etcdstore.Versioned[T], error) {
	index, err := store.Get(ctx, indexKey)
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	if index.Entry == nil {
		return etcdstore.Versioned[T]{}, errs.New(notFound, "resource was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(idKind, id) != nil {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindInternal, "slug index contains an invalid stable id")
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{primaryKey(id)}, Revision: index.ReadRevision})
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	if len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindInternal, "slug index references a missing primary record")
	}
	record, err := decode(result.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[T]{}, err
	}
	if identity(record) != id {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindInternal, "slug index id does not match its primary record")
	}
	if !matches(record) {
		return etcdstore.Versioned[T]{}, errs.New(errs.KindInternal, "slug index ownership does not match its primary record")
	}
	return etcdstore.Versioned[T]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}
