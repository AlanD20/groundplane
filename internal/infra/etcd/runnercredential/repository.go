// Package runnercredential persists revocable Runner credentials as digests.
package runnercredential

import (
	"context"
	"crypto/sha256"
	"time"

	identity "github.com/AlanD20/groundplane/internal/common/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	Get(context.Context, string) (*keyvalue.GetResult, error)
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct {
	store Store
	now   func() time.Time
}

func New(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Runner credential storage is required")
	}
	return &Repository{store: store, now: time.Now}, nil
}

// ReadRunnerCredential returns an empty binding for unknown, expired, revoked,
// or superseded credentials. The caller still resolves action grants from the
// current registration and approved workflow.
func (repository *Repository) ReadRunnerCredential(
	ctx context.Context,
	digest [sha256.Size]byte,
) (identity.Binding, error) {
	binding, _, err := repository.readLive(ctx, digest)
	return binding, err
}

// AdmissionConditions fences the exact authenticated credential and live
// registration in the same transaction as the action's durable publication.
// Callers combine these conditions with their authoritative grant and resource
// revisions. Expiry is evaluated when admission is prepared.
func (repository *Repository) AdmissionConditions(
	ctx context.Context, digest [sha256.Size]byte, expected identity.Binding,
) ([]keyvalue.Condition, error) {
	binding, conditions, err := repository.readLive(ctx, digest)
	if err != nil {
		return nil, err
	}
	if binding.Revision <= 0 || binding != expected {
		return nil, errs.New(errs.KindRequestForbidden, "Runner credential is no longer active")
	}
	return conditions, nil
}

func (repository *Repository) readLive(
	ctx context.Context, digest [sha256.Size]byte,
) (identity.Binding, []keyvalue.Condition, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return identity.Binding{}, nil, err
	}
	if digest == ([sha256.Size]byte{}) {
		return identity.Binding{}, nil, nil
	}
	key := credentialKey(digest)
	read, err := repository.store.Get(ctx, key)
	if err != nil {
		return identity.Binding{}, nil, err
	}
	if read == nil || read.ReadRevision <= 0 {
		return identity.Binding{}, nil, corrupt()
	}
	if read.Entry == nil {
		return identity.Binding{}, nil, nil
	}
	defer clear(read.Entry.Value)
	if read.Entry.ModRevision > read.ReadRevision {
		return identity.Binding{}, nil, corrupt()
	}
	record, err := decodeCredential(read.Entry, key)
	if err != nil {
		return identity.Binding{}, nil, err
	}
	keys := registrationKeys(record.RunnerID)
	values, err := repository.read(ctx, keys, read.ReadRevision)
	if err != nil {
		return identity.Binding{}, nil, err
	}
	defer keyvalue.ClearValues(values.Values)
	registration, live, err := liveRegistration(values.Values, record.RunnerID, record.RunnerEpoch)
	binding := record.binding(read.Entry.ModRevision)
	if err != nil || !live || !binding.ActiveAt(repository.now()) {
		return identity.Binding{}, nil, err
	}
	conditions := append(registration, keyvalue.Condition{Key: key, ModRevision: read.Entry.ModRevision})
	return binding, conditions, nil
}

func registrationKeys(runnerID string) []string {
	return []string{
		runners.RunnerKey(runnerID),
		runners.RunnerLifecycleKey(runnerID),
		deletions.TombstoneKey(string(deletions.DeletionTargetRunner), runnerID),
	}
}

func liveRegistration(
	values []*keyvalue.KeyValue, runnerID string, epoch uint64,
) ([]keyvalue.Condition, bool, error) {
	desired, lifecycle, tombstone := values[0], values[1], values[2]
	if tombstone != nil || desired == nil && lifecycle == nil {
		return nil, false, nil
	}
	record, err := runners.DecodeRunnerAggregate(desired, lifecycle)
	if err != nil || record.Desired.ID != runnerID || runners.ValidateRunnerRecord(record) != nil {
		return nil, false, corrupt()
	}
	if record.ProvisioningState != runners.RunnerProvisioningReady || record.RuntimeEpoch != epoch {
		return nil, false, nil
	}
	return []keyvalue.Condition{
		{Key: desired.Key, ModRevision: desired.ModRevision},
		{Key: lifecycle.Key, ModRevision: lifecycle.ModRevision},
		{Key: registrationKeys(runnerID)[2]},
	}, true, nil
}

func (repository *Repository) read(
	ctx context.Context,
	keys []string,
	revision int64,
) (*keyvalue.GetManyResult, error) {
	result, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, corrupt()
	}
	if result.ReadRevision <= 0 || revision > 0 && result.ReadRevision != revision || len(result.Values) != len(keys) {
		keyvalue.ClearValues(result.Values)
		return nil, corrupt()
	}
	for index, value := range result.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > result.ReadRevision) {
			keyvalue.ClearValues(result.Values)
			return nil, corrupt()
		}
	}
	return result, nil
}
