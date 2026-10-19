package hostbindings

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func LocalIdentityKey() string { return localHostKey }

// ReadLocalIdentity reads the stable MVP host even while no Agent is enrolled.
// Missing identity proves only that this installation has never enrolled a host;
// it is not a fallback to Controller-local runtime or an execution grant.
func ReadLocalIdentity(ctx context.Context, store Reader, revision int64) (keyvalue.Versioned[string], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[string]{}, err
	}
	if store == nil || revision < 0 {
		return keyvalue.Versioned[string]{}, errs.New(errs.KindValidationFailed, "local host identity read is invalid")
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{localHostKey}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[string]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return keyvalue.Versioned[string]{}, errs.New(errs.KindInternal, "local host identity read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return keyvalue.Versioned[string]{ReadRevision: read.ReadRevision}, nil
	}
	if value.Key != localHostKey || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
		return keyvalue.Versioned[string]{}, errs.New(errs.KindInternal, "local host identity revision is invalid")
	}
	host, err := recordcodec.Decode[localHost](value.Value, "local-host")
	if err != nil || ids.Validate(ids.KindHost, host.ID) != nil {
		return keyvalue.Versioned[string]{}, errs.New(errs.KindInternal, "local host identity is corrupt")
	}
	return keyvalue.Versioned[string]{
		Record:       host.ID,
		Revision:     value.ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}
