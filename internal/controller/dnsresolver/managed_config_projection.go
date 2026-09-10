package dnsresolver

import (
	"context"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolverBaselineReader is the read-only durable baseline authority used by
// managed-file preview. It intentionally has no capture or persistence method.
type ResolverBaselineReader interface {
	GetHostResolverBaseline(
		context.Context,
	) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error)
}

// ManagedConfigProjector derives the CoreDNS managed file from durable desired
// state and already-persisted resolver inputs without publishing state.
type ManagedConfigProjector struct {
	projections PlatformProjectionReader
	baselines   ResolverBaselineReader
	renderer    componentdns.Renderer
	path        string
}

func NewManagedConfigProjector(
	projections PlatformProjectionReader,
	baselines ResolverBaselineReader,
	renderer componentdns.Renderer,
	path string,
) (*ManagedConfigProjector, error) {
	if projections == nil || baselines == nil || renderer == nil || path == "" {
		return nil, errs.New(errs.KindInternal, "managed-config projector dependencies are required")
	}
	return &ManagedConfigProjector{
		projections: projections, baselines: baselines, renderer: renderer, path: path,
	}, nil
}

func (projector *ManagedConfigProjector) ProjectManagedConfigFiles(
	ctx context.Context,
	component core.Component,
	_ int64,
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
	)
	if err != nil {
		return nil, err
	}
	config, err := DecodeConfig(component.Config)
	if err != nil {
		return nil, err
	}
	input, err := BuildRenderInput(
		resolverInput.HostResolution.Hosts,
		config,
		resolverInput.Baseline.Resolvers,
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
	return []apiTypes.ManagedConfigFile{{
		Path: projector.path, Template: config.CorefileTemplate, Rendered: renderedText,
	}}, nil
}
