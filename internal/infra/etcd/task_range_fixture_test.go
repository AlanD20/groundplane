package etcd

import (
	"context"
	"sort"
	"strings"
)

func (store *memoryTaskStore) Range(
	ctx context.Context,
	request RangeRequest,
) (*RangeResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	keys := make([]string, 0)
	for key := range store.history {
		pastStart := request.StartExclusive == "" || (!request.Descending && key > request.StartExclusive) ||
			(request.Descending && key < request.StartExclusive)
		if !strings.HasPrefix(key, request.Prefix) || !pastStart ||
			store.valueAtLocked(key, revision) == nil {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if request.Descending {
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
	}
	more := int64(len(keys)) > request.Limit
	if more {
		keys = keys[:request.Limit]
	}
	values := make([]KeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, *store.valueAtLocked(key, revision))
	}
	return &RangeResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision, More: more,
	}, nil
}
