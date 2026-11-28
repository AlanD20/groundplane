// Package remotehosts persists remote Host credentials and binds their authority
// to assignment and execution ownership in etcd.
package remotehosts

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const recordKind = "remote-host"

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type Repository struct {
	store Store
}

func NewRepository(store Store) *Repository {
	return &Repository{store: store}
}

// Key is the credential authority guard for requests from a remote Agent.
// Request handlers must join this key's authorized revision to protected writes.
func Key(hostID string) string { return "/v1/remote-hosts/" + hostID }

func (repository *Repository) CreateHost(ctx context.Context, host remotehost.Host) error {
	if host.Agent != nil || host.Pending != nil || host.Generation != 0 {
		return errs.New(errs.KindValidationFailed, "New Remote Host already contains enrollment state")
	}
	created, err := repository.write(ctx, 0, host, workqueue.Change{})
	if err != nil {
		return err
	}
	if !created {
		return recordcodec.StateConflict(recordKind, host.ID)
	}
	return nil
}

func (repository *Repository) ReadHost(ctx context.Context, hostID string) (remotehost.VersionedHost, error) {
	host, _, err := repository.readHost(ctx, hostID)
	return host, err
}

func (repository *Repository) readHost(ctx context.Context, hostID string) (remotehost.VersionedHost, int64, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return remotehost.VersionedHost{}, 0, err
	}
	if err := recordcodec.ValidateID(ids.KindHost, hostID); err != nil {
		return remotehost.VersionedHost{}, 0, err
	}
	key := Key(hostID)
	value, readRevision, err := repository.read(ctx, key, 0)
	if err != nil {
		return remotehost.VersionedHost{}, 0, err
	}
	if value == nil {
		return remotehost.VersionedHost{}, 0, errs.New(errs.KindRequestNotFound, "Remote Host was not found")
	}
	defer clear(value.Value)
	host, err := DecodeHost(hostID, value.Value)
	if err != nil {
		return remotehost.VersionedHost{}, 0, err
	}
	return remotehost.VersionedHost{Host: host, Revision: value.ModRevision}, readRevision, nil
}

func DecodeHost(hostID string, value []byte) (remotehost.Host, error) {
	host, err := recordcodec.Decode[remotehost.Host](value, recordKind)
	if err != nil || host.ID != hostID || host.Validate() != nil {
		return remotehost.Host{}, recordcodec.CorruptRecord()
	}
	return host, nil
}

func (repository *Repository) read(ctx context.Context, key string, revision int64) (*keyvalue.KeyValue, int64, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, 0, err
	}
	if read == nil {
		return nil, 0, recordcodec.CorruptRecord()
	}
	if len(read.Values) != 1 || read.ReadRevision <= 0 || (revision > 0 && read.ReadRevision != revision) {
		keyvalue.ClearValues(read.Values)
		return nil, 0, recordcodec.CorruptRecord()
	}
	value := read.Values[0]
	if value != nil && (value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision) {
		clear(value.Value)
		return nil, 0, recordcodec.CorruptRecord()
	}
	return value, read.ReadRevision, nil
}

func (repository *Repository) write(
	ctx context.Context,
	expectedRevision int64,
	host remotehost.Host,
	change workqueue.Change,
) (bool, error) {
	defer keyvalue.ClearMutationValues(change.Mutations)
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, err
	}
	if err := host.Validate(); err != nil {
		return false, err
	}
	value, err := recordcodec.Encode(recordKind, host)
	if err != nil {
		return false, err
	}
	defer clear(value)
	key := Key(host.ID)
	conditions := append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: expectedRevision})
	mutations := append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: value})
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	return result.Succeeded, err
}
