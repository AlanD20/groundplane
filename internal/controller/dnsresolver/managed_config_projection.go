package dnsresolver

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	resolverbaseline "github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"net/netip"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolverBaselineReader is the read-only durable baseline authority used by
// managed-file preview. It intentionally has no capture or persistence method.
type ResolverBaselineReader interface {
	GetHostResolverBaseline(
		context.Context,
	) (etcdstore.Versioned[resolverbaseline.Record], bool, error)
}

// ManagedConfigProjector derives the CoreDNS managed file from durable desired
// state and already-persisted resolver inputs without publishing state.
type ManagedConfigProjector struct {
	records         DNSRecordsResolver
	projections     PlatformProjectionReader
	baselines       ResolverBaselineReader
	renderer        componentdns.Renderer
	path            string
	registryAddress netip.Addr
}

func NewManagedConfigProjector(
	projections PlatformProjectionReader,
	records DNSRecordsResolver,
	baselines ResolverBaselineReader,
	renderer componentdns.Renderer,
	path string,
	registryAddress netip.Addr,
) (*ManagedConfigProjector, error) {
	if projections == nil || records == nil || baselines == nil || renderer == nil || path == "" {
		return nil, errs.New(errs.KindInternal, "managed-config projector dependencies are required")
	}
	return &ManagedConfigProjector{
		records:     records,
		projections: projections, baselines: baselines, renderer: renderer, path: path, registryAddress: registryAddress,
	}, nil
}

func (projector *ManagedConfigProjector) ProjectManagedConfigFiles(
	ctx context.Context,
	component core.Component,
	revision int64,
) ([]apiTypes.ManagedConfigFile, error) {
	files := []apiTypes.ManagedConfigFile{}
	if !component.Enabled || component.Config.CoreDNS == nil {
		return files, nil
	}
	baseline, found, err := projector.baselines.GetHostResolverBaseline(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "host resolver baseline is not initialized")
	}
	resolvers, err := componentdns.ParseResolverBaseline(baseline.Record.Content)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	projection, found, err := projector.projections.GetHostResolutionProjection(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindStateConflict, "host-resolution projection is not initialized")
	}
	resolverInput, _, err := resolverInputFromProjection(
		projection.Record,
		baseline.Record.Generation,
		resolvers,
		projector.registryAddress,
	)
	if err != nil {
		return nil, err
	}
	config, err := DecodeConfig(component.Config)
	if err != nil {
		return nil, err
	}
	hosts, _, err := projector.records.ResolveDNSRecords(ctx, component.Config.CoreDNS.Records, revision)
	if err != nil {
		return nil, err
	}
	resolverInput.HostResolution, _, err = mergeResolverHosts(resolverInput.HostResolution, hosts)
	if err != nil {
		return nil, err
	}
	input, err := BuildRenderInput(
		resolverInput.HostResolution.Hosts,
		config,
		resolverInput.Baseline.Resolvers,
		resolverInput.PrivateListener,
	)
	if err != nil {
		return nil, err
	}
	rendered, err := projector.renderer.Render(input)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	renderedText := string(rendered)
	clear(rendered)
	// Use the same renderer with only its insertion marker to expose the exact
	// generated directives; the Console must not reconstruct DNS configuration.
	input.CorefileTemplate = "{groundplane}\n"
	directives, err := projector.renderer.Render(input)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	generated := string(directives)
	clear(directives)
	return []apiTypes.ManagedConfigFile{{
		Path: projector.path, Template: config.CorefileTemplate, Rendered: renderedText,
		GeneratedDirectives: generated,
	}}, nil
}
