package etcd

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// EnvironmentBlueprintStore preserves the closed atomic desired-state
// publication capability without exposing retired hidden-Task transactions.
type EnvironmentBlueprintStore interface {
	etcdstore.Store
	TransactEnvironmentBlueprint(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionResult, error)
}
