package app

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// initializeExecutionSourceReferences completes private preparation recovery
// during construction, before Run starts HTTP, Agent dispatch, or schedulers.
// The source module owns the durable transitions and bounded release batches.
func initializeExecutionSourceReferences(
	ctx context.Context,
	store etcdstore.Store,
) (*scriptsourcepublication.Authority, error) {
	if err := etcd.RecoverTaskSecretPinSources(ctx, store); err != nil {
		return nil, err
	}
	authority, err := scriptsourcepublication.NewAuthority(store)
	if err != nil {
		return nil, err
	}
	if err := authority.RecoverPreparations(ctx); err != nil {
		return nil, err
	}
	return authority, nil
}
