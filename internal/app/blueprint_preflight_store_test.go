package app

import (
	"context"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// blueprintPreflightStore is an in-memory persistence witness. Initialization
// uses the real repositories; the apply phase prohibits every write entry point.
type blueprintPreflightStore struct {
	values   map[string]etcd.KeyValue
	revision int64
	readOnly bool
	writes   int
}

func (s *blueprintPreflightStore) Health(context.Context) error { return nil }
func (s *blueprintPreflightStore) Close() error                 { return nil }
func (s *blueprintPreflightStore) Get(_ context.Context, key string) (*etcd.GetResult, error) {
	result := &etcd.GetResult{ReadRevision: s.revision}
	if value, found := s.values[key]; found {
		value.Value = append([]byte(nil), value.Value...)
		result.Entry = &value
	}
	return result, nil
}

func (s *blueprintPreflightStore) GetMany(
	ctx context.Context,
	request etcd.GetManyRequest,
) (*etcd.GetManyResult, error) {
	result := &etcd.GetManyResult{ReadRevision: s.revision, ResponseRevision: s.revision}
	for _, key := range request.Keys {
		value, err := s.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		result.Values = append(result.Values, value.Entry)
	}
	return result, nil
}
func (s *blueprintPreflightStore) Range(_ context.Context, request etcd.RangeRequest) (*etcd.RangeResult, error) {
	result := &etcd.RangeResult{ReadRevision: s.revision, ResponseRevision: s.revision}
	for key, value := range s.values {
		if strings.HasPrefix(key, request.Prefix) && (request.StartExclusive == "" || key > request.StartExclusive) {
			value.Value = append([]byte(nil), value.Value...)
			result.Values = append(result.Values, value)
		}
	}
	sort.Slice(result.Values, func(i, j int) bool { return result.Values[i].Key < result.Values[j].Key })
	if request.Limit > 0 && len(result.Values) > int(request.Limit) {
		result.More = true
		result.Values = result.Values[:request.Limit]
	}
	return result, nil
}
func (s *blueprintPreflightStore) Put(ctx context.Context, key string, value []byte) (int64, error) {
	result, err := s.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationPut, Key: key, Value: value}})
	return result.Revision, err
}
func (s *blueprintPreflightStore) Delete(ctx context.Context, key string) (int64, error) {
	result, err := s.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationDelete, Key: key}})
	return result.Revision, err
}

func (s *blueprintPreflightStore) Transact(
	_ context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionResult, error) {
	if len(mutations) != 0 {
		s.writes++
		if s.readOnly {
			return etcd.TransactionResult{}, errors.New("durable write before image preflight succeeded")
		}
	}
	for _, condition := range conditions {
		if s.values[condition.Key].ModRevision != condition.ModRevision {
			return etcd.TransactionResult{Revision: s.revision}, nil
		}
		if condition.Prefix {
			for key := range s.values {
				if strings.HasPrefix(key, condition.Key) {
					return etcd.TransactionResult{Revision: s.revision}, nil
				}
			}
		}
	}
	if len(mutations) != 0 {
		s.revision++
	}
	for _, mutation := range mutations {
		if mutation.Type == etcd.MutationPut {
			prior := s.values[mutation.Key]
			s.values[mutation.Key] = etcd.KeyValue{
				Key:         mutation.Key,
				Value:       append([]byte(nil), mutation.Value...),
				Version:     prior.Version + 1,
				ModRevision: s.revision,
			}
		} else {
			delete(s.values, mutation.Key)
		}
	}
	return etcd.TransactionResult{Succeeded: true, Revision: s.revision}, nil
}

func (s *blueprintPreflightStore) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []etcd.Condition,
	mutations []etcd.Mutation,
) (etcd.TransactionResult, error) {
	return s.Transact(ctx, conditions, mutations)
}
func (*blueprintPreflightStore) ValidateBlueprintTaskTerminal(
	_ context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) error {
	return envelope.ValidateBudget("")
}
func (s *blueprintPreflightStore) TransactBlueprintTaskTerminal(
	ctx context.Context,
	envelope etcd.BlueprintTaskTerminalTransaction,
) (etcd.TransactionResult, error) {
	conditions, mutations, err := envelope.Operations()
	if err != nil {
		return etcd.TransactionResult{}, err
	}
	return s.Transact(ctx, conditions, mutations)
}
func (*blueprintPreflightStore) Watch(context.Context, string, int64) (*etcd.WatchStream, error) {
	return nil, errors.New("unexpected watch")
}
func (*blueprintPreflightStore) Snapshot(context.Context, io.Writer) error {
	return errors.New("unexpected snapshot")
}
