package dnsresolver

import (
	"context"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	resolverbaseline "github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
	"time"
)

type PlatformProjectionReader interface {
	GetHostResolutionProjection(
		context.Context,
	) (etcdstore.Versioned[resolutionrecord.HostResolutionProjectionRecord], bool, error)
}

type BaselineRepository interface {
	GetHostResolverBaseline(context.Context) (etcdstore.Versioned[resolverbaseline.Record], bool, error)
	EnsureHostResolverBaseline(
		context.Context,
		[]byte,
		time.Time,
	) (etcdstore.Versioned[resolverbaseline.Record], error)
}

type BaselineCapture func(context.Context) ([]byte, error)

type ActionCatalog interface {
	Digest() [32]byte
	FindAction(
		componentsdk.ImplementationKey,
		componentsdk.ActionID,
	) (componentsdk.Definition, componentsdk.ActionDefinition, bool)
	FindActionByCapability(
		componentsdk.Capability,
		componentsdk.ActionID,
	) (componentsdk.Definition, componentsdk.ActionDefinition, bool)
}

type PlatformRenderPlanner struct {
	zoneListeners       *ZoneListenerSource
	records             DNSRecordsResolver
	projections         PlatformProjectionReader
	baselines           BaselineRepository
	capture             BaselineCapture
	renderer            componentdns.Renderer
	environmentPlanner  EnvironmentPlanner
	catalog             ActionCatalog
	managedConfigAction componentsdk.ActionID
	registryAddress     netip.Addr
}

func NewPlatformRenderPlanner(
	projections PlatformProjectionReader,
	records DNSRecordsResolver,
	baselines BaselineRepository,
	capture BaselineCapture,
	renderer componentdns.Renderer,
	environmentPlanner EnvironmentPlanner,
	catalog ActionCatalog,
	managedConfigAction componentsdk.ActionID,
	registryAddress netip.Addr,
	zoneListeners *ZoneListenerSource,
) (*PlatformRenderPlanner, error) {
	if projections == nil || records == nil || zoneListeners == nil || baselines == nil ||
		capture == nil ||
		renderer == nil ||
		environmentPlanner == nil ||
		catalog == nil {
		return nil, errs.New(errs.KindInternal, "platform Component render planner dependencies are required")
	}
	return &PlatformRenderPlanner{
		zoneListeners: zoneListeners,
		records:       records,
		projections:   projections, baselines: baselines, capture: capture,
		renderer: renderer, environmentPlanner: environmentPlanner, catalog: catalog,
		managedConfigAction: managedConfigAction,
		registryAddress:     registryAddress,
	}, nil
}
