package componentaction

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/docker/dnsresolverobserver"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Runtime authenticates read-only connection checks against the compiled catalog.
// Native operation handlers own Component mutation and recovery separately.
type Runtime struct {
	catalog  Catalog
	observer dnsResolverObserver
}

type dnsResolverObserver interface {
	CheckConnections(
		context.Context,
		dnsresolverobserver.Request,
		[]dnsresolverobserver.ConnectionQuery,
	) (dnsresolverobserver.ConnectionCheckResult, error)
}

func New(catalog Catalog, observer dnsResolverObserver) (*Runtime, error) {
	if catalog == nil || observer == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Component connection checks require the compiled catalog and DNS observer",
		)
	}
	return &Runtime{catalog: catalog, observer: observer}, nil
}
