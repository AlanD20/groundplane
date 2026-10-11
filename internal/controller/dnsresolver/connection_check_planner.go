package dnsresolver

import (
	"context"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ConnectionCheckPlanner binds the operator's query selection to captured
// acknowledged files and the one compiled resolver observation action.
type ConnectionCheckPlanner struct {
	components *etcd.ComponentRepository
	catalog    ActionCatalog
	action     componentsdk.ActionID
}

func NewConnectionCheckPlanner(components *etcd.ComponentRepository, catalog ActionCatalog,
	action componentsdk.ActionID,
) (*ConnectionCheckPlanner, error) {
	if components == nil || catalog == nil || action == "" {
		return nil, errs.New(
			errs.KindInternal,
			"DNS check Component repository and compiled catalog action are required",
		)
	}
	if _, _, found := catalog.FindActionByCapability(componentsdk.CapabilityDNSResolver, action); !found {
		return nil, errs.New(errs.KindInternal, "DNS check observation action is absent from the compiled catalog")
	}
	return &ConnectionCheckPlanner{components: components, catalog: catalog, action: action}, nil
}

func (planner *ConnectionCheckPlanner) Capture(ctx context.Context, componentID string, revision int64,
	queries []*agentpb.DNSConnectionQuery,
) (etcd.DNSCheckSource, *agentpb.CheckDNSConnections, error) {
	source, err := planner.components.LoadDNSCheckSource(ctx, componentID, revision)
	if err != nil {
		return etcd.DNSCheckSource{}, nil, err
	}
	definition, action, found := planner.catalog.FindActionByCapability(
		componentsdk.CapabilityDNSResolver,
		planner.action,
	)
	if !found {
		return etcd.DNSCheckSource{}, nil, errs.New(
			errs.KindInternal,
			"DNS check compiled observation action is unavailable",
		)
	}
	definitionDigest, catalogDigest := definition.Digest(), planner.catalog.Digest()
	request, err := source.Request(&agentpb.ComponentApply{
		DefinitionDigest: append(
			[]byte(nil),
			definitionDigest[:]...), CatalogDigest: append([]byte(nil), catalogDigest[:]...),
		ActionId: string(action.ID()),
	}, queries)
	if err != nil {
		return etcd.DNSCheckSource{}, nil, err
	}
	return source, request, nil
}
