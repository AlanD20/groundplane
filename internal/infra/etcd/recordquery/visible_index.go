package recordquery

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ListVisibleIndex preserves the fixed-revision owner-index cursor while
// excluding private records. matches still validates every index owner; only
// visible may skip a valid record.
func ListVisibleIndex[T any](
	ctx context.Context,
	store store,
	collection, ownerKind, ownerID, prefix string,
	primaryKey func(string) string,
	idKind ids.Kind,
	request etcdstore.PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches, visible func(T) bool,
) (etcdstore.Page[T], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Page[T]{}, err
	}
	limit, revision, start, query, err := NormalizePageRequest(
		request, collection, ownerKind, ownerID, prefix, idKind,
	)
	if err != nil {
		return etcdstore.Page[T]{}, err
	}
	items := make([]etcdstore.Versioned[T], 0, limit)
	lastVisibleKey := ""
	for {
		indexed, rangeErr := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: int64(etcdstore.MaximumPageLimit), Revision: revision,
		})
		if rangeErr != nil {
			return etcdstore.Page[T]{}, rangeErr
		}
		if indexed.ReadRevision <= 0 || revision > 0 && indexed.ReadRevision != revision {
			ClearRangeKeyValues(indexed.Values)
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "indexed list changed revision")
		}
		revision = indexed.ReadRevision
		keys := make([]string, len(indexed.Values))
		idsByIndex := make([]string, len(indexed.Values))
		for index, value := range indexed.Values {
			if ValidateListKey(prefix, value.Key, idKind) != nil {
				ClearRangeKeyValues(indexed.Values)
				return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index contains an invalid key")
			}
			id := strings.TrimPrefix(value.Key, prefix)
			if string(value.Value) != id {
				ClearRangeKeyValues(indexed.Values)
				return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index value does not match its key")
			}
			keys[index], idsByIndex[index] = primaryKey(id), id
		}
		if len(keys) != 0 {
			primaries, readErr := GetManyBatchedAtRevision(ctx, store, keys, revision)
			if readErr != nil {
				ClearRangeKeyValues(indexed.Values)
				return etcdstore.Page[T]{}, readErr
			}
			for index, value := range primaries.Values {
				if value == nil {
					etcdstore.ClearValues(primaries.Values)
					ClearRangeKeyValues(indexed.Values)
					return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index references a missing primary record")
				}
				record, decodeErr := decode(value.Value)
				if decodeErr != nil {
					etcdstore.ClearValues(primaries.Values)
					ClearRangeKeyValues(indexed.Values)
					return etcdstore.Page[T]{}, decodeErr
				}
				if identity(record) != idsByIndex[index] || !matches(record) {
					etcdstore.ClearValues(primaries.Values)
					ClearRangeKeyValues(indexed.Values)
					return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index does not match its primary record")
				}
				if !visible(record) {
					continue
				}
				if len(items) == limit {
					cursor, cursorErr := nextPageCursor(&etcdstore.RangeResult{
						Values: []etcdstore.KeyValue{{Key: lastVisibleKey}}, More: true, ReadRevision: revision,
					}, query, idKind, prefix)
					etcdstore.ClearValues(primaries.Values)
					ClearRangeKeyValues(indexed.Values)
					if cursorErr != nil {
						return etcdstore.Page[T]{}, cursorErr
					}
					return etcdstore.Page[T]{Items: items, NextCursor: cursor, Revision: revision}, nil
				}
				items = append(items, etcdstore.Versioned[T]{
					Record: record, Revision: value.ModRevision, ReadRevision: revision,
				})
				lastVisibleKey = indexed.Values[index].Key
			}
			etcdstore.ClearValues(primaries.Values)
		}
		more := indexed.More
		if len(indexed.Values) != 0 {
			start = indexed.Values[len(indexed.Values)-1].Key
		}
		ClearRangeKeyValues(indexed.Values)
		if !more {
			return etcdstore.Page[T]{Items: items, Revision: revision}, nil
		}
		if len(keys) == 0 {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "indexed list returned an empty continuation")
		}
	}
}
