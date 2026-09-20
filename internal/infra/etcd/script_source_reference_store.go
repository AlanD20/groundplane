package etcd

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"math"

	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type scriptSourceReferenceStore struct{ store hierarchyStore }

func (adapter *scriptSourceReferenceStore) GetMany(
	ctx context.Context,
	keys []string,
	revision int64,
) (*ref.GetManyResult, error) {
	read, err := adapter.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil || read == nil {
		return nil, err
	}
	result := &ref.GetManyResult{Values: make([]*ref.KeyValue, len(read.Values)), ReadRevision: read.ReadRevision}
	for index, value := range read.Values {
		if value != nil {
			result.Values[index] = &ref.KeyValue{
				Key:         value.Key,
				Value:       append([]byte(nil), value.Value...),
				ModRevision: value.ModRevision,
			}
		}
	}
	return result, nil
}

func (adapter *scriptSourceReferenceStore) Range(
	ctx context.Context,
	prefix string,
	limit int64,
) (*ref.RangeResult, error) {
	read, err := adapter.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: limit})
	if err != nil || read == nil {
		return nil, err
	}
	result := &ref.RangeResult{
		Values:       make([]ref.KeyValue, len(read.Values)),
		ReadRevision: read.ReadRevision,
		More:         read.More,
	}
	for index, value := range read.Values {
		result.Values[index] = ref.KeyValue{
			Key:         value.Key,
			Value:       append([]byte(nil), value.Value...),
			ModRevision: value.ModRevision,
		}
	}
	return result, nil
}

func (adapter *scriptSourceReferenceStore) Transact(
	ctx context.Context,
	conditions []ref.Condition,
	mutations []ref.Mutation,
) (ref.TransactionResult, error) {
	result, err := adapter.store.Transact(
		ctx,
		convertScriptSourceConditions(conditions),
		convertScriptSourceMutations(mutations),
	)
	return ref.TransactionResult{Succeeded: result.Succeeded, Revision: result.Revision}, err
}

func (adapter *scriptSourceReferenceStore) AdjustScriptPrimary(
	value []byte,
	source ref.SourceIdentity,
	delta int64,
) ([]byte, error) {
	record, err := scriptrecord.DecodeRecord(value)
	if err != nil || source.Kind != ScriptSourceBody || record.EnvironmentID != source.EnvironmentID ||
		record.ScriptSetGeneration != source.ScriptSetGeneration || record.Desired.ID != source.ScriptID {
		return nil, errs.New(errs.KindInternal, "Script source primary is corrupt")
	}
	stored, err := recordcodec.Decode[scriptrecord.StoredRecord](value, "script")
	if err != nil || stored.ActiveReferences != record.ActiveReferences {
		return nil, errs.New(errs.KindInternal, "Script source primary is corrupt")
	}
	if delta > 0 {
		if uint64(delta) > math.MaxUint64-stored.ActiveReferences {
			return nil, errs.New(errs.KindStateConflict, "Script active reference count is exhausted")
		}
		stored.ActiveReferences += uint64(delta)
	} else {
		decrement := uint64(-delta)
		if stored.ActiveReferences < decrement {
			return nil, errs.New(errs.KindInternal, "Script active reference count underflowed")
		}
		stored.ActiveReferences -= decrement
	}
	return recordcodec.Encode("script", stored)
}

func convertScriptSourceConditions(input []ref.Condition) []etcdstore.Condition {
	result := make([]etcdstore.Condition, len(input))
	for index, condition := range input {
		result[index] = etcdstore.Condition{Key: condition.Key, ModRevision: condition.ModRevision, Prefix: condition.Prefix}
	}
	return result
}

func convertScriptSourceMutations(input []ref.Mutation) []etcdstore.Mutation {
	result := make([]etcdstore.Mutation, len(input))
	for index, mutation := range input {
		result[index] = etcdstore.Mutation{
			Type:   etcdstore.MutationType(mutation.Type),
			Key:    mutation.Key,
			Value:  append([]byte(nil), mutation.Value...),
			Prefix: mutation.Prefix,
		}
	}
	return result
}

func mapScriptSourceReferenceError(err error) error {
	if err == nil {
		return nil
	}
	var typed *ref.Error
	if !errors.As(err, &typed) {
		return err
	}
	switch typed.Kind {
	case ref.ErrorValidation:
		return errs.New(errs.KindValidationFailed, typed.Text)
	case ref.ErrorConflict:
		return errs.New(errs.KindStateConflict, typed.Text)
	default:
		return errs.New(errs.KindInternal, typed.Text)
	}
}
