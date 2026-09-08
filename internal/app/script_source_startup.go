package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// initializeScriptSourceReferences completes private preparation recovery
// during construction, before Run starts HTTP, Agent dispatch, or schedulers.
// The source module owns the durable transitions and bounded release batches.
func initializeScriptSourceReferences(
	ctx context.Context,
	store etcd.Store,
) (*etcd.ScriptSourceReferenceAuthority, error) {
	authority, err := etcd.NewScriptSourceReferenceAuthority(store)
	if err != nil {
		return nil, err
	}
	if err := authority.RecoverPreparations(ctx); err != nil {
		return nil, err
	}
	return authority, nil
}
