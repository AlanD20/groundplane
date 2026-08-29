package dnsresolver

import (
	"context"
	"encoding/hex"
	"net/netip"
	"sort"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type PlatformProjectionReader interface {
	ListEnvironmentAppliedComposeProjections(
		context.Context,
	) ([]etcd.Versioned[etcd.EnvironmentComposeProjection], error)
}

type BaselineRepository interface {
	GetHostResolverBaseline(context.Context) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error)
	EnsureHostResolverBaseline(context.Context, []byte, time.Time) (etcd.Versioned[etcd.HostResolverBaselineRecord], error)
}

type BaselineCapture func(context.Context) ([]byte, error)

type ActionCatalog interface {
	Digest() [32]byte
	FindAction(componentsdk.ImplementationKey, componentsdk.ActionID) (componentsdk.Definition, componentsdk.ActionDefinition, bool)
}

type PlatformRenderPlanner struct {
	projections         PlatformProjectionReader
	baselines           BaselineRepository
	capture             BaselineCapture
	renderer            componentdns.Renderer
	environmentPlanner  EnvironmentPlanner
	catalog             ActionCatalog
	managedConfigAction componentsdk.ActionID
}

func NewPlatformRenderPlanner(
	projections PlatformProjectionReader,
	baselines BaselineRepository,
	capture BaselineCapture,
	renderer componentdns.Renderer,
	environmentPlanner EnvironmentPlanner,
	catalog ActionCatalog,
	managedConfigAction componentsdk.ActionID,
) (*PlatformRenderPlanner, error) {
	if projections == nil || baselines == nil || capture == nil || renderer == nil || environmentPlanner == nil || catalog == nil {
		return nil, errs.New(errs.KindInternal, "platform Component render planner dependencies are required")
	}
	return &PlatformRenderPlanner{
		projections: projections, baselines: baselines, capture: capture,
		renderer: renderer, environmentPlanner: environmentPlanner, catalog: catalog,
		managedConfigAction: managedConfigAction,
	}, nil
}

func (planner *PlatformRenderPlanner) PrepareConfigTask(
	ctx context.Context,
	current etcd.Versioned[etcd.ComponentRecord],
	desired core.Component,
	task etcd.TaskRecord,
) (etcd.PlatformComponentTaskRenderInput, error) {
	if err := ensureHostResolverBaseline(ctx, planner.baselines, planner.capture, time.Now().UTC()); err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	baseline, found, err := planner.baselines.GetHostResolverBaseline(ctx)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	if !found {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"host resolver baseline is not initialized",
		)
	}
	resolvers, err := componentdns.ParseResolverBaseline(baseline.Record.Content)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	hosts, durableHosts, err := planner.appliedHosts(ctx)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	generatedServiceID := ""
	switch len(desired.GeneratedServices) {
	case 0:
		generatedServiceID = ids.New(ids.KindService)
	case 1:
		generatedServiceID = desired.GeneratedServices[0]
	default:
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS Component has more than one generated Service",
		)
	}
	renderComponent := desired
	renderComponent.GeneratedServices = []string{generatedServiceID}
	intent, err := BuildIntent(
		planner.renderer, planner.environmentPlanner, renderComponent, hosts, resolvers,
	)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	replacement, err := etcd.ReplaceComponentDesired(current.Record, desired)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	desiredSHA256, err := etcd.PlatformComponentDesiredDigest(replacement)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	definition, action, found := planner.catalog.FindAction(
		componentsdk.ImplementationKey(core.ComponentKindCoreDNS),
		planner.managedConfigAction,
	)
	if !found {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindInternal,
			"CoreDNS activate-config action is absent from the compiled catalog",
		)
	}
	config := core.CloneComponentConfig(desired.Config).CoreDNS
	if config == nil {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindInternal,
			"CoreDNS typed config disappeared during planning",
		)
	}
	definitionDigest := definition.Digest()
	catalogDigest := planner.catalog.Digest()
	return etcd.PlatformComponentTaskRenderInput{
		PlanID: task.PlanID, TaskID: task.ID, ComponentID: desired.ID,
		DesiredSHA256:      desiredSHA256,
		BaselineGeneration: baseline.Record.Generation, BaselineSHA256: baseline.Record.SHA256,
		Config: *config, Hosts: durableHosts, GeneratedServiceID: generatedServiceID,
		EnsureService:    len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy,
		DefinitionSHA256: hex.EncodeToString(definitionDigest[:]),
		CatalogSHA256:    hex.EncodeToString(catalogDigest[:]), ActionID: string(action.ID()),
		ArtifactID: ids.New(ids.KindConfig), ComposeArtifactID: ids.New(ids.KindConfig),
		ArtifactSHA256: hex.EncodeToString(intent.ArtifactSHA256[:]),
		ArtifactLength: intent.ArtifactLength,
		PlanSHA256:     hex.EncodeToString(intent.PlanSHA256[:]),
	}, nil
}

func (planner *PlatformRenderPlanner) PrepareDisableTask(
	ctx context.Context,
	current etcd.Versioned[etcd.ComponentRecord],
	desired core.Component,
	task etcd.TaskRecord,
) (etcd.PlatformComponentTaskRenderInput, error) {
	currentComponent, err := etcd.ProjectComponentRecord(current.Record)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	if !currentComponent.Enabled || len(currentComponent.GeneratedServices) != 1 ||
		currentComponent.Config.CoreDNS == nil || desired.Enabled {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS must be applied before it can be disabled",
		)
	}
	input, err := planner.PrepareConfigTask(ctx, current, currentComponent, task)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	replacement, err := etcd.ReplaceComponentDesired(current.Record, desired)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	input.DesiredSHA256, err = etcd.PlatformComponentDesiredDigest(replacement)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	input.EnsureService = false
	input.DisableService = true
	return input, nil
}

func (planner *PlatformRenderPlanner) appliedHosts(
	ctx context.Context,
) ([]componentdns.Host, []etcd.PlatformDNSHost, error) {
	projections, err := planner.projections.ListEnvironmentAppliedComposeProjections(ctx)
	if err != nil {
		return nil, nil, err
	}
	byAddress := make(map[string]map[string]struct{})
	for _, projection := range projections {
		for _, record := range projection.Record.Components {
			if record.Desired.Kind != core.ComponentKindIngressCaddy || !record.Desired.Enabled ||
				!record.Runtime.Healthy || record.Runtime.PinnedIPv4 == "" {
				continue
			}
			names := byAddress[record.Runtime.PinnedIPv4]
			if names == nil {
				names = make(map[string]struct{})
				byAddress[record.Runtime.PinnedIPv4] = names
			}
			for _, route := range projection.Record.Routes {
				if route.Host != "" {
					names[route.Host] = struct{}{}
				}
			}
		}
	}
	durable := make([]etcd.PlatformDNSHost, 0, len(byAddress))
	for address, names := range byAddress {
		hostnames := make([]string, 0, len(names))
		for hostname := range names {
			hostnames = append(hostnames, hostname)
		}
		sort.Strings(hostnames)
		durable = append(durable, etcd.PlatformDNSHost{Address: address, Hostnames: hostnames})
	}
	sort.Slice(durable, func(left, right int) bool {
		leftAddress, leftErr := netip.ParseAddr(durable[left].Address)
		rightAddress, rightErr := netip.ParseAddr(durable[right].Address)
		if leftErr != nil || rightErr != nil {
			return durable[left].Address < durable[right].Address
		}
		return leftAddress.Compare(rightAddress) < 0
	})
	hosts := make([]componentdns.Host, len(durable))
	for index, host := range durable {
		address, err := netip.ParseAddr(host.Address)
		if err != nil {
			return nil, nil, errs.New(errs.KindInternal, "applied Caddy address is invalid")
		}
		hosts[index] = componentdns.Host{Address: address, Hostnames: append([]string(nil), host.Hostnames...)}
	}
	return hosts, durable, nil
}

func ensureHostResolverBaseline(
	ctx context.Context,
	repository BaselineRepository,
	capture BaselineCapture,
	now time.Time,
) error {
	if _, found, err := repository.GetHostResolverBaseline(ctx); err != nil || found {
		return err
	}
	content, err := capture(ctx)
	if err != nil {
		return err
	}
	defer clear(content)
	_, err = repository.EnsureHostResolverBaseline(ctx, content, now)
	return err
}
