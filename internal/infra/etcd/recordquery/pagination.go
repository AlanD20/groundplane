package recordquery

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func NormalizePageRequest(
	request etcdstore.PageRequest,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	idKind ids.Kind,
) (int, int64, string, string, error) {
	limit := request.Limit
	if limit == 0 {
		limit = etcdstore.DefaultPageLimit
	}
	if limit < 1 || limit > etcdstore.MaximumPageLimit {
		return 0, 0, "", "", errs.New(
			errs.KindValidationFailed,
			"page limit must be between 1 and 200",
		)
	}
	if request.Revision < 0 {
		return 0, 0, "", "", errs.New(
			errs.KindValidationFailed,
			"page revision must not be negative",
		)
	}
	query, err := recordcodec.CursorQueryDigest(collection, ownerKind, ownerID, limit)
	if err != nil {
		return 0, 0, "", "", err
	}
	if request.Cursor == "" {
		return limit, request.Revision, "", query, nil
	}
	cursor, err := recordcodec.DecodeCursor(request.Cursor)
	if err != nil {
		return 0, 0, "", "", err
	}
	if cursor.Query != query {
		return 0, 0, "", "", errs.New(errs.KindMalformedRequest, "cursor does not match the list query")
	}
	if request.Revision > 0 && request.Revision != cursor.Revision {
		return 0, 0, "", "", errs.New(errs.KindMalformedRequest, "cursor does not match the fixed revision")
	}
	if ids.Validate(idKind, cursor.LastID) != nil {
		return 0, 0, "", "", errs.New(errs.KindMalformedRequest, "cursor contains an invalid last id")
	}
	return limit, cursor.Revision, prefix + cursor.LastID, query, nil
}

func ValidateListKey(prefix string, key string, kind ids.Kind) error {
	if !strings.HasPrefix(key, prefix) {
		return fmt.Errorf("key is outside prefix")
	}
	id := strings.TrimPrefix(key, prefix)
	if strings.Contains(id, "/") {
		return fmt.Errorf("key has nested segments")
	}
	return ids.Validate(kind, id)
}

func ListPrimary[T any](
	ctx context.Context,
	store store,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	idKind ids.Kind,
	request etcdstore.PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
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
	rangeResult, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: start, Limit: int64(limit), Revision: revision,
	})
	if err != nil {
		return etcdstore.Page[T]{}, err
	}
	defer ClearRangeKeyValues(rangeResult.Values)
	items := make([]etcdstore.Versioned[T], 0, len(rangeResult.Values))
	for _, value := range rangeResult.Values {
		if err := ValidateListKey(prefix, value.Key, idKind); err != nil {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "primary list contains an invalid key")
		}
		record, err := decode(value.Value)
		if err != nil {
			return etcdstore.Page[T]{}, err
		}
		expectedID := strings.TrimPrefix(value.Key, prefix)
		if identity(record) != expectedID {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "primary list key does not match its record id")
		}
		if !matches(record) {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "primary list contains a record outside its scope")
		}
		items = append(items, etcdstore.Versioned[T]{
			Record: record, Revision: value.ModRevision, ReadRevision: rangeResult.ReadRevision,
		})
	}
	next, err := nextPageCursor(rangeResult, query, idKind, prefix)
	if err != nil {
		return etcdstore.Page[T]{}, err
	}
	return etcdstore.Page[T]{Items: items, NextCursor: next, Revision: rangeResult.ReadRevision}, nil
}

func ListFilteredPrimary[T any](
	ctx context.Context,
	store store,
	collection string,
	filterKind string,
	filterID string,
	prefix string,
	idKind ids.Kind,
	request etcdstore.PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
) (etcdstore.Page[T], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Page[T]{}, err
	}
	limit, revision, start, query, err := NormalizePageRequest(
		request, collection, filterKind, filterID, prefix, idKind,
	)
	if err != nil {
		return etcdstore.Page[T]{}, err
	}
	items := make([]etcdstore.Versioned[T], 0, limit)
	continuations := make([]etcdstore.KeyValue, 0, limit)
	readRevision := revision
	for {
		rangeResult, err := store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: int64(etcdstore.MaximumPageLimit), Revision: readRevision,
		})
		if err != nil {
			return etcdstore.Page[T]{}, err
		}
		if rangeResult.ReadRevision <= 0 || (readRevision > 0 && rangeResult.ReadRevision != readRevision) {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "filtered primary list changed revision")
		}
		readRevision = rangeResult.ReadRevision
		for _, value := range rangeResult.Values {
			if err := ValidateListKey(prefix, value.Key, idKind); err != nil {
				return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "filtered primary list contains an invalid key")
			}
			record, err := decode(value.Value)
			if err != nil {
				return etcdstore.Page[T]{}, err
			}
			expectedID := strings.TrimPrefix(value.Key, prefix)
			if identity(record) != expectedID {
				return etcdstore.Page[T]{}, errs.New(
					errs.KindInternal,
					"filtered primary list key does not match its record id",
				)
			}
			if !matches(record) {
				continue
			}
			if len(items) == limit {
				next, err := nextPageCursor(&etcdstore.RangeResult{
					Values: continuations, More: true, ReadRevision: readRevision,
				}, query, idKind, prefix)
				if err != nil {
					return etcdstore.Page[T]{}, err
				}
				return etcdstore.Page[T]{Items: items, NextCursor: next, Revision: readRevision}, nil
			}
			items = append(items, etcdstore.Versioned[T]{
				Record: record, Revision: value.ModRevision, ReadRevision: readRevision,
			})
			continuations = append(continuations, value)
		}
		if !rangeResult.More {
			return etcdstore.Page[T]{Items: items, Revision: readRevision}, nil
		}
		if len(rangeResult.Values) == 0 {
			return etcdstore.Page[T]{}, errs.New(
				errs.KindInternal,
				"filtered primary list returned an empty continuation",
			)
		}
		start = rangeResult.Values[len(rangeResult.Values)-1].Key
	}
}

func ListIndex[T any](
	ctx context.Context,
	store store,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	primaryKey func(string) string,
	idKind ids.Kind,
	request etcdstore.PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
) (etcdstore.Page[T], error) {
	return ListIndexAtRevision(
		ctx, store, collection, ownerKind, ownerID, prefix, primaryKey, idKind,
		request, decode, identity, matches, 0,
	)
}

func ListIndexAtRevision[T any](
	ctx context.Context,
	store store,
	collection string,
	ownerKind string,
	ownerID string,
	prefix string,
	primaryKey func(string) string,
	idKind ids.Kind,
	request etcdstore.PageRequest,
	decode func([]byte) (T, error),
	identity func(T) string,
	matches func(T) bool,
	anchorRevision int64,
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
	if revision == 0 {
		revision = anchorRevision
	} else if anchorRevision > 0 && revision != anchorRevision {
		return etcdstore.Page[T]{}, errs.New(errs.KindStateConflict, "list cursor Script-set generation changed")
	}
	rangeResult, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix, StartExclusive: start, Limit: int64(limit), Revision: revision,
	})
	if err != nil {
		return etcdstore.Page[T]{}, err
	}
	defer ClearRangeKeyValues(rangeResult.Values)
	if len(rangeResult.Values) == 0 {
		return etcdstore.Page[T]{Items: []etcdstore.Versioned[T]{}, Revision: rangeResult.ReadRevision}, nil
	}
	primaryKeys := make([]string, len(rangeResult.Values))
	expectedIDs := make([]string, len(rangeResult.Values))
	for index, value := range rangeResult.Values {
		if err := ValidateListKey(prefix, value.Key, idKind); err != nil {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index contains an invalid key")
		}
		id := strings.TrimPrefix(value.Key, prefix)
		if string(value.Value) != id {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index value does not match its key")
		}
		primaryKeys[index] = primaryKey(id)
		expectedIDs[index] = id
	}
	primaries, err := GetManyBatchedAtRevision(ctx, store, primaryKeys, rangeResult.ReadRevision)
	if err != nil {
		return etcdstore.Page[T]{}, err
	}
	defer etcdstore.ClearValues(primaries.Values)
	if len(primaries.Values) != len(primaryKeys) {
		return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index read returned an invalid primary count")
	}
	items := make([]etcdstore.Versioned[T], 0, len(primaryKeys))
	for index, value := range primaries.Values {
		if value == nil {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index references a missing primary record")
		}
		record, err := decode(value.Value)
		if err != nil {
			return etcdstore.Page[T]{}, err
		}
		if identity(record) != expectedIDs[index] {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index id does not match its primary record")
		}
		if !matches(record) {
			return etcdstore.Page[T]{}, errs.New(errs.KindInternal, "owner index does not match its primary record")
		}
		items = append(items, etcdstore.Versioned[T]{
			Record: record, Revision: value.ModRevision, ReadRevision: rangeResult.ReadRevision,
		})
	}
	next, err := nextPageCursor(rangeResult, query, idKind, prefix)
	if err != nil {
		return etcdstore.Page[T]{}, err
	}
	return etcdstore.Page[T]{Items: items, NextCursor: next, Revision: rangeResult.ReadRevision}, nil
}

func GetManyBatchedAtRevision(
	ctx context.Context,
	store store,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || revision <= 0 {
		return nil, errs.New(errs.KindInternal, "fixed-revision batched read is invalid")
	}
	values := make([]*etcdstore.KeyValue, 0, len(keys))
	responseRevision := int64(0)
	for start := 0; start < len(keys); start += etcdstore.MaximumOperations {
		end := min(start+etcdstore.MaximumOperations, len(keys))
		batch, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys[start:end], Revision: revision})
		if err != nil {
			if batch != nil {
				etcdstore.ClearValues(batch.Values)
			}
			etcdstore.ClearValues(values)
			return nil, err
		}
		if batch == nil || batch.ReadRevision != revision || len(batch.Values) != end-start {
			if batch != nil {
				etcdstore.ClearValues(batch.Values)
			}
			etcdstore.ClearValues(values)
			return nil, errs.New(errs.KindInternal, "fixed-revision batched read is incomplete")
		}
		values = append(values, batch.Values...)
		responseRevision = max(responseRevision, batch.ResponseRevision)
	}
	return &etcdstore.GetManyResult{
		Values: values, ReadRevision: revision, ResponseRevision: responseRevision,
	}, nil
}

func ClearRangeKeyValues(values []etcdstore.KeyValue) {
	for index := range values {
		clear(values[index].Value)
	}
}

func nextPageCursor(result *etcdstore.RangeResult, query string, kind ids.Kind, prefix string) (string, error) {
	if !result.More {
		return "", nil
	}
	if len(result.Values) == 0 || result.ReadRevision <= 0 {
		return "", errs.New(errs.KindInternal, "paginated range returned an invalid continuation")
	}
	lastKey := result.Values[len(result.Values)-1].Key
	if err := ValidateListKey(prefix, lastKey, kind); err != nil {
		return "", errs.New(errs.KindInternal, "paginated range returned an invalid continuation key")
	}
	lastID := strings.TrimPrefix(lastKey, prefix)
	return recordcodec.EncodeCursor(recordcodec.Cursor{
		Version: recordcodec.CursorVersion, Revision: result.ReadRevision, LastID: lastID, Query: query,
	})
}
