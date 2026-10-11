package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
)

func (ledger *ReleaseLedger) ReadBlueprintProxyReplacementImage(ctx context.Context,
	environmentID, parentID, serviceID string, revision int64) (*componentplanning.ProxyReplacementImage, error) {
	return componentplanning.ReadBlueprintProxyReplacementImage(
		ctx,
		ledger.store,
		environmentID,
		parentID,
		serviceID,
		revision,
	)
}

func (ledger *ReleaseLedger) ReadBlueprintProxyServiceRolloutReference(ctx context.Context,
	environmentID, parentID, serviceID string, revision int64,
) (*componentplanning.ProxyServiceRolloutReference, error) {
	return componentplanning.ReadBlueprintProxyServiceRolloutReference(
		ctx,
		ledger.store,
		environmentID,
		parentID,
		serviceID,
		revision,
	)
}
