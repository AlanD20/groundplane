package dnsresolver

import (
	"context"
	"encoding/hex"
	"net/netip"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type PlatformProjectionReader interface {
	GetHostResolutionProjection(context.Context) (etcd.Versioned[etcd.HostResolutionProjectionRecord], bool, error)
}

type BaselineRepository interface {
	GetHostResolverBaseline(context.Context) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error)
	EnsureHostResolverBaseline(context.Context, []byte, time.Time) (etcd.Versioned[etcd.HostResolverBaselineRecord], error)
}

type BaselineCapture func(context.Context) ([]byte, error)

type ActionCatalog interface {
	Digest() [32]byte
	FindAction(componentsdk.ImplementationKey, componentsdk.ActionID) (componentsdk.Definition, componentsdk.ActionDefinition, bool)
	FindActionByCapability(componentsdk.Capability, componentsdk.ActionID) (componentsdk.Definition, componentsdk.ActionDefinition, bool)
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

type fixedProjectionReader struct {
	record etcd.HostResolutionProjectionRecord
}

func (reader fixedProjectionReader) GetHostResolutionProjection(
	context.Context,
) (etcd.Versioned[etcd.HostResolutionProjectionRecord], bool, error) {
	return etcd.Versioned[etcd.HostResolutionProjectionRecord]{Record: reader.record}, true, nil
}

// PrepareConfigTaskAtProjection is the startup/recovery seam. It uses the
// same planner as an ordinary Component reconciliation while pinning the
// caller's exact projection snapshot instead of reading a second view.
func (planner *PlatformRenderPlanner) PrepareConfigTaskAtProjection(
	ctx context.Context,
	current etcd.Versioned[etcd.ComponentRecord],
	desired core.Component,
	task etcd.TaskRecord,
	projection etcd.HostResolutionProjectionRecord,
) (etcd.PlatformComponentTaskRenderInput, error) {
	clone := *planner
	clone.projections = fixedProjectionReader{record: projection}
	return clone.PrepareConfigTask(ctx, current, desired, task)
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
	hostResolution, found, err := planner.projections.GetHostResolutionProjection(ctx)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	if !found {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(errs.KindStateConflict, "host-resolution projection is not initialized")
	}
	definition, action, found := planner.catalog.FindActionByCapability(
		componentsdk.CapabilityDNSResolver, planner.managedConfigAction,
	)
	if !found || !definitionProvidesResolverGrants(definition) {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindInternal,
			"registered dns-resolver capability is absent from the compiled catalog",
		)
	}
	resolverInput, durableHosts, err := resolverInputFromProjection(hostResolution.Record, baseline.Record.Generation, resolvers)
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
			"dns-resolver Component has more than one generated Service",
		)
	}
	renderComponent := desired
	renderComponent.GeneratedServices = []string{generatedServiceID}
	intent, err := BuildIntent(
		planner.renderer, planner.environmentPlanner, renderComponent, resolverInput, definition.Implementation(),
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
		HostResolutionInputRevision: hostResolution.Record.InputRevision,
		HostResolutionSHA256:        hostResolution.Record.InputSHA256,
		Config:                      *config, Hosts: durableHosts, GeneratedServiceID: generatedServiceID,
		EnsureService:    len(current.Record.Runtime.GeneratedServices) == 0 || !current.Record.Runtime.Healthy,
		DefinitionSHA256: hex.EncodeToString(definitionDigest[:]),
		CatalogSHA256:    hex.EncodeToString(catalogDigest[:]), ActionID: string(action.ID()),
		ArtifactID: ids.New(ids.KindConfig), ComposeArtifactID: ids.New(ids.KindConfig),
		ArtifactSHA256: hex.EncodeToString(intent.ArtifactSHA256[:]),
		ArtifactLength: intent.ArtifactLength,
		PlanSHA256:     hex.EncodeToString(intent.PlanSHA256[:]),
	}, nil
}

// SelectResolver selects the sole platform-owned Component after proving that
// the compiled catalog contains a resolver definition with its generic grants.
func (planner *PlatformRenderPlanner) SelectResolver(
	ctx context.Context,
	candidates []etcd.Versioned[etcd.ComponentRecord],
) (etcd.Versioned[etcd.ComponentRecord], error) {
	if ctx == nil || planner == nil || planner.catalog == nil {
		return etcd.Versioned[etcd.ComponentRecord]{}, errs.New(errs.KindInternal, "dns-resolver selector dependencies are required")
	}
	definition, _, found := planner.catalog.FindActionByCapability(
		componentsdk.CapabilityDNSResolver, planner.managedConfigAction,
	)
	if !found || !definitionProvidesResolverGrants(definition) {
		return etcd.Versioned[etcd.ComponentRecord]{}, errs.New(errs.KindInternal, "registered dns-resolver capability is absent from the compiled catalog")
	}
	var selected etcd.Versioned[etcd.ComponentRecord]
	for _, candidate := range candidates {
		if candidate.Record.Desired.Owner != core.ComponentOwnerPlatform || candidate.Record.Desired.OwnerID != "" {
			continue
		}
		if selected.Revision != 0 {
			return etcd.Versioned[etcd.ComponentRecord]{}, errs.New(errs.KindStateConflict, "multiple platform dns-resolver Components are registered")
		}
		selected = candidate
	}
	if selected.Revision <= 0 || selected.ReadRevision <= 0 {
		return etcd.Versioned[etcd.ComponentRecord]{}, errs.New(errs.KindComponentNotFound, "platform dns-resolver Component is not registered")
	}
	return selected, nil
}

func definitionProvidesResolverGrants(definition componentsdk.Definition) bool {
	providesResolver := false
	for _, capability := range definition.Provides() {
		providesResolver = providesResolver || capability == componentsdk.CapabilityDNSResolver
	}
	grants := func(capability componentsdk.Capability, operation componentsdk.Operation) bool {
		for _, grant := range definition.Grants() {
			if grant.Capability() != capability {
				continue
			}
			for _, granted := range grant.Operations() {
				if granted == operation {
					return true
				}
			}
		}
		return false
	}
	return providesResolver && grants(componentsdk.CapabilityManagedConfig, componentsdk.OperationConfigure) &&
		grants(componentsdk.CapabilityManagedConfig, componentsdk.OperationActivate) &&
		grants(componentsdk.CapabilityHostResolution, componentsdk.OperationConfigure) &&
		grants(componentsdk.CapabilityHostResolution, componentsdk.OperationObserve)
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

func resolverInputFromProjection(
	record etcd.HostResolutionProjectionRecord,
	baselineGeneration uint64,
	resolvers []componentdns.ResolverEndpoint,
) (componentdns.ResolverInput, []etcd.PlatformDNSHost, error) {
	baseline, err := componentdns.NewResolverBaseline(baselineGeneration, resolvers)
	if err != nil {
		return componentdns.ResolverInput{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	digestBytes, err := hex.DecodeString(record.InputSHA256)
	if err != nil || len(digestBytes) != 32 {
		return componentdns.ResolverInput{}, nil, errs.New(errs.KindInternal, "host-resolution projection digest is corrupt")
	}
	var digest [32]byte
	copy(digest[:], digestBytes)
	byAddress := make(map[netip.Addr][]string)
	for _, route := range record.Routes {
		address, parseErr := netip.ParseAddr(route.IPv4)
		if parseErr != nil {
			return componentdns.ResolverInput{}, nil, errs.New(errs.KindInternal, "host-resolution address is corrupt")
		}
		byAddress[address] = append(byAddress[address], route.Hostname)
	}
	hosts := make([]componentdns.Host, 0, len(byAddress))
	for address, names := range byAddress {
		hosts = append(hosts, componentdns.Host{Address: address, Hostnames: names})
	}
	projection, err := componentdns.NewHostResolutionProjection(record.InputRevision, digest, hosts)
	if err != nil {
		return componentdns.ResolverInput{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	durable := make([]etcd.PlatformDNSHost, len(projection.Hosts))
	for index, host := range projection.Hosts {
		durable[index] = etcd.PlatformDNSHost{Address: host.Address.String(), Hostnames: append([]string(nil), host.Hostnames...)}
	}
	return componentdns.ResolverInput{Baseline: baseline, HostResolution: projection}, durable, nil
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
