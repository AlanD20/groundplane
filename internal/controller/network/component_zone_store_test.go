package network

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"io"
	"sort"
	"strings"
)

type componentZoneStore struct {
	revision int64
	values   map[string]*testkeyvalue.KeyValue
}

func (s *componentZoneStore) Health(context.Context) error { return nil }
func (s *componentZoneStore) Close() error                 { return nil }
func (s *componentZoneStore) Watch(context.Context, string, int64) (*testkeyvalue.WatchStream, error) {
	return nil, fmt.Errorf("unexpected watch")
}
func (s *componentZoneStore) Snapshot(context.Context, io.Writer) error {
	return fmt.Errorf("unexpected snapshot")
}
func (s *componentZoneStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return etcd.MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}
func (s *componentZoneStore) Get(_ context.Context, key string) (*testkeyvalue.GetResult, error) {
	return &testkeyvalue.GetResult{Entry: s.copyAt(key, s.revision), ReadRevision: s.revision}, nil
}
func (s *componentZoneStore) copyAt(key string, revision int64) *testkeyvalue.KeyValue {
	v := s.values[key]
	if v == nil {
		return nil
	}
	if v.ModRevision > revision {
		panic("fixture attempted unsupported overwritten historical read")
	}
	c := *v
	c.Value = append([]byte(nil), v.Value...)
	return &c
}

func (s *componentZoneStore) GetMany(
	_ context.Context,
	r testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	revision := r.Revision
	if revision == 0 {
		revision = s.revision
	}
	v := make([]*testkeyvalue.KeyValue, len(r.Keys))
	for i, k := range r.Keys {
		v[i] = s.copyAt(k, revision)
	}
	return &testkeyvalue.GetManyResult{Values: v, ReadRevision: revision, ResponseRevision: s.revision}, nil
}

func (s *componentZoneStore) Range(
	_ context.Context,
	r testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	revision := r.Revision
	if revision == 0 {
		revision = s.revision
	}
	keys := []string{}
	for k := range s.values {
		if strings.HasPrefix(k, r.Prefix) && k > r.StartExclusive {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	out := &testkeyvalue.RangeResult{ReadRevision: revision, ResponseRevision: s.revision}
	for _, k := range keys {
		if r.Limit > 0 && int64(len(out.Values)) == r.Limit {
			out.More = true
			break
		}
		out.Values = append(out.Values, *s.copyAt(k, revision))
	}
	return out, nil
}
func (s *componentZoneStore) Put(ctx context.Context, k string, v []byte) (int64, error) {
	r, e := s.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: k, Value: v}})
	return r.Revision, e
}
func (s *componentZoneStore) Delete(ctx context.Context, k string) (int64, error) {
	r, e := s.Transact(ctx, nil, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: k}})
	return r.Revision, e
}

func (s *componentZoneStore) Transact(
	_ context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, c := range conditions {
		actual := int64(0)
		if v := s.values[c.Key]; v != nil {
			actual = v.ModRevision
		}
		if c.Prefix {
			for k := range s.values {
				if strings.HasPrefix(k, c.Key) {
					actual = s.revision
					break
				}
			}
		}
		if actual != c.ModRevision {
			values := make([]*testkeyvalue.KeyValue, len(conditions))
			for i, c := range conditions {
				values[i] = s.copyAt(c.Key, s.revision)
			}
			return testkeyvalue.TransactionResult{Revision: s.revision, FailureReads: values}, nil
		}
	}
	s.revision++
	for _, m := range mutations {
		if m.Prefix {
			if m.Type != testkeyvalue.MutationDelete {
				panic("unsupported prefix put")
			}
			for key := range s.values {
				if strings.HasPrefix(key, m.Key) {
					delete(s.values, key)
				}
			}
			continue
		}
		if m.Type == testkeyvalue.MutationDelete {
			delete(s.values, m.Key)
			continue
		}
		version := int64(1)
		if old := s.values[m.Key]; old != nil {
			version = old.Version + 1
		}
		s.values[m.Key] = &testkeyvalue.KeyValue{
			Key:         m.Key,
			Value:       append([]byte(nil), m.Value...),
			ModRevision: s.revision,
			Version:     version,
		}
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: s.revision}, nil
}
