package tasksecretpins

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type etcdStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

type recoverySecretPinStore struct {
	store     etcdStore
	projectID string
}

func NewEtcdRepository(store etcdStore, projectID string) (*Repository, error) {
	return NewRepository(recoverySecretPinStore{store: store, projectID: projectID})
}

func (adapter recoverySecretPinStore) GetMany(
	ctx context.Context, keys []string, revision int64,
) (*GetManyResult, error) {
	read, err := adapter.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil || read == nil {
		return nil, err
	}
	result := &GetManyResult{ReadRevision: read.ReadRevision,
		Values: make([]*KeyValue, len(read.Values))}
	for index, value := range read.Values {
		if value != nil {
			result.Values[index] = &KeyValue{
				Key: value.Key, Value: value.Value, ModRevision: value.ModRevision,
			}
		}
	}
	return result, nil
}

func (adapter recoverySecretPinStore) Range(
	ctx context.Context, prefix string, limit int64,
) (*RangeResult, error) {
	read, err := adapter.store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: limit})
	if err != nil || read == nil {
		return nil, err
	}
	result := &RangeResult{ReadRevision: read.ReadRevision, More: read.More,
		Values: make([]KeyValue, len(read.Values))}
	for index, value := range read.Values {
		result.Values[index] = KeyValue{
			Key: value.Key, Value: value.Value, ModRevision: value.ModRevision,
		}
	}
	return result, nil
}

func (adapter recoverySecretPinStore) Transact(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	compares, writes, err := EtcdFragment(
		Fragment{Conditions: conditions, Mutations: mutations},
	)
	if err != nil {
		return TransactionResult{}, err
	}
	result, err := adapter.store.Transact(ctx, compares, writes)
	clearKeyValues(result.FailureReads)
	return TransactionResult{Succeeded: result.Succeeded, Revision: result.Revision}, err
}

func EtcdFragment(fragment Fragment) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	conditions := make([]etcdstore.Condition, len(fragment.Conditions))
	for index, condition := range fragment.Conditions {
		conditions[index] = etcdstore.Condition{
			Key:         condition.Key,
			ModRevision: condition.ModRevision,
			Prefix:      condition.Prefix,
		}
	}
	mutations := make([]etcdstore.Mutation, len(fragment.Mutations))
	for index, mutation := range fragment.Mutations {
		var kind etcdstore.MutationType
		switch mutation.Type {
		case MutationPut:
			kind = etcdstore.MutationPut
		case MutationDelete:
			kind = etcdstore.MutationDelete
		default:
			return nil, nil, errs.New(errs.KindInternal, "unknown recovery Secret pin mutation")
		}
		mutations[index] = etcdstore.Mutation{
			Type:   kind,
			Key:    mutation.Key,
			Value:  mutation.Value,
			Prefix: mutation.Prefix,
		}
	}
	return conditions, mutations, nil
}

func (adapter recoverySecretPinStore) VerifySecret(
	ctx context.Context, pin tasksecretpinrecord.Record,
) (SecretAuthority, error) {
	if err := tasksecretpinrecord.Validate(pin); err != nil {
		return SecretAuthority{}, err
	}
	keys := []string{secretrecord.RecordKey(pin.SecretID), secretrecord.ValueKey(pin.SecretID)}
	read, err := adapter.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return SecretAuthority{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 2 {
		return SecretAuthority{}, errs.New(
			errs.KindInternal,
			"recovery Secret source read is incomplete",
		)
	}
	defer clearKeyValues(read.Values)
	metadata, value := read.Values[0], read.Values[1]
	if metadata == nil || value == nil || metadata.Key != keys[0] || value.Key != keys[1] ||
		metadata.ModRevision != pin.MetadataRevision || value.ModRevision <= 0 {
		return SecretAuthority{}, errs.New(
			errs.KindStateConflict,
			"pinned recovery Secret is unavailable",
		)
	}
	record, err := secretrecord.DecodeRecord(metadata.Value)
	if err != nil || record.Secret.ID != pin.SecretID {
		return SecretAuthority{}, secretrecord.CorruptRecord()
	}
	encrypted, err := secretrecord.DecodeEncryptedValue(value.Value)
	defer clear(encrypted.Ciphertext)
	if err != nil || encrypted.SecretID != pin.SecretID || encrypted.CiphertextSHA256 != pin.CiphertextSHA256 {
		return SecretAuthority{}, errs.New(
			errs.KindStateConflict,
			"pinned recovery Secret value changed",
		)
	}
	authority := SecretAuthority{
		MetadataRevision: metadata.ModRevision,
		ValueRevision:    value.ModRevision,
	}
	if record.Secret.Scope == core.SecretScopePlatform {
		return authority, nil
	}
	if record.Secret.ProjectID != adapter.projectID {
		return SecretAuthority{}, errs.New(
			errs.KindStateConflict,
			"recovery Secret belongs to another Project",
		)
	}
	ownerKey := hierarchyrecord.ProjectKey(adapter.projectID)
	owners, err := adapter.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{ownerKey}, Revision: read.ReadRevision},
	)
	if err != nil {
		return SecretAuthority{}, err
	}
	if owners == nil || owners.ReadRevision != read.ReadRevision || len(owners.Values) != 1 || owners.Values[0] == nil {
		return SecretAuthority{}, errs.New(
			errs.KindStateConflict,
			"recovery Secret Project is unavailable",
		)
	}
	defer clearKeyValues(owners.Values)
	project, err := hierarchyrecord.DecodeProject(owners.Values[0].Value)
	if err != nil || project.ID != adapter.projectID || owners.Values[0].Key != ownerKey {
		return SecretAuthority{}, recordcodec.CorruptRecord()
	}
	authority.ProjectID, authority.TenantID = project.ID, project.TenantID
	return authority, nil
}

func clearKeyValues(values []*etcdstore.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
			value.Value = nil
		}
	}
}
