package dnsresolver

import (
	"context"
	"encoding/hex"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	resolverbaseline "github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"net/netip"
	"runtime"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PlatformProjectionReader interface {
	GetHostResolutionProjection(context.Context) (etcdstore.Versioned[etcd.HostResolutionProjectionRecord], bool, error)
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

type ObservationRepository interface {
	GetPlatformComponentObservation(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.ComponentObservationRecord], bool, error)
}

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
	projections         PlatformProjectionReader
	baselines           BaselineRepository
	observations        ObservationRepository
	capture             BaselineCapture
	renderer            componentdns.Renderer
	environmentPlanner  EnvironmentPlanner
	catalog             ActionCatalog
	managedConfigAction componentsdk.ActionID
	bootstrapProvenance bool
}

type fixedProjectionReader struct {
	record etcd.HostResolutionProjectionRecord
}

type fixedObservationReader struct {
	componentID string
	record      etcd.ComponentObservationRecord
}

func (reader fixedProjectionReader) GetHostResolutionProjection(
	context.Context,
) (etcdstore.Versioned[etcd.HostResolutionProjectionRecord], bool, error) {
	return etcdstore.Versioned[etcd.HostResolutionProjectionRecord]{Record: reader.record}, true, nil
}

func (reader fixedObservationReader) GetPlatformComponentObservation(
	_ context.Context,
	componentID string,
) (etcdstore.Versioned[etcd.ComponentObservationRecord], bool, error) {
	if componentID != reader.componentID {
		return etcdstore.Versioned[etcd.ComponentObservationRecord]{}, false, nil
	}
	return etcdstore.Versioned[etcd.ComponentObservationRecord]{Record: reader.record}, true, nil
}

// PrepareConfigTaskAtProjection is the startup/recovery seam. It uses the
// same planner as an ordinary Component reconciliation while pinning the
// caller's exact projection snapshot instead of reading a second view.
func (planner *PlatformRenderPlanner) PrepareConfigTaskAtProjection(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task etcd.TaskRecord,
	projection etcd.HostResolutionProjectionRecord,
	priorObservation *etcd.ComponentObservationRecord,
) (etcd.PlatformComponentTaskRenderInput, error) {
	clone := *planner
	clone.projections = fixedProjectionReader{record: projection}
	if priorObservation != nil {
		clone.observations = fixedObservationReader{componentID: desired.ID, record: *priorObservation}
	}
	return clone.PrepareConfigTask(ctx, current, desired, task)
}

// PrepareBootstrapConfigTaskAtProjection is reserved for the clean-start seam,
// whose repository caller has proved same-revision singleton provenance.
func (planner *PlatformRenderPlanner) PrepareBootstrapConfigTaskAtProjection(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task etcd.TaskRecord,
	projection etcd.HostResolutionProjectionRecord,
) (etcd.PlatformComponentTaskRenderInput, error) {
	clone := *planner
	clone.bootstrapProvenance = true
	return clone.PrepareConfigTaskAtProjection(ctx, current, desired, task, projection, nil)
}

func NewPlatformRenderPlanner(
	projections PlatformProjectionReader,
	baselines BaselineRepository,
	observations ObservationRepository,
	capture BaselineCapture,
	renderer componentdns.Renderer,
	environmentPlanner EnvironmentPlanner,
	catalog ActionCatalog,
	managedConfigAction componentsdk.ActionID,
) (*PlatformRenderPlanner, error) {
	if projections == nil || baselines == nil || observations == nil || capture == nil || renderer == nil ||
		environmentPlanner == nil ||
		catalog == nil {
		return nil, errs.New(errs.KindInternal, "platform Component render planner dependencies are required")
	}
	return &PlatformRenderPlanner{
		projections: projections, baselines: baselines, observations: observations, capture: capture,
		renderer: renderer, environmentPlanner: environmentPlanner, catalog: catalog,
		managedConfigAction: managedConfigAction,
	}, nil
}

func (planner *PlatformRenderPlanner) PrepareConfigTask(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task etcd.TaskRecord,
) (etcd.PlatformComponentTaskRenderInput, error) {
	return planner.prepareConfigTask(ctx, current, desired, task, false)
}

func (planner *PlatformRenderPlanner) prepareConfigTask(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task etcd.TaskRecord,
	disableService bool,
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
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"host-resolution projection is not initialized",
		)
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
	resolverInput, durableHosts, err := resolverInputFromProjection(
		hostResolution.Record,
		baseline.Record.Generation,
		resolvers,
	)
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
	replacement, err := componentrecord.ReplaceDesired(current.Record, desired)
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
	decodedConfig, err := DecodeConfig(desired.Config)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	selectedRenderInput, err := BuildRenderInput(
		resolverInput.HostResolution.Hosts, decodedConfig, resolverInput.Baseline.Resolvers,
	)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	selectedPlan, err := planner.environmentPlanner.Plan(
		definition.Implementation(), generatedServiceID, selectedRenderInput,
	)
	if err != nil || len(selectedPlan.Services) != 1 {
		clearEnvironmentPlan(selectedPlan)
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindInternal,
			"registered resolver image is unavailable",
		)
	}
	image := selectedPlan.Services[0].Image
	selectedOS, selectedArch := runtime.GOOS, runtime.GOARCH
	selectedPlatform, selectedReference, selected := image.Select(selectedOS, selectedArch)
	defer clearEnvironmentPlan(selectedPlan)
	if !selected || selectedOS != "linux" {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindValidationFailed,
			"component platform is unsupported",
		)
	}
	definitionDigest := definition.Digest()
	catalogDigest := planner.catalog.Digest()
	ensureService, err := componentTaskEnsureService(task)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	observation, observationFound, err := planner.observations.GetPlatformComponentObservation(ctx, desired.ID)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	priorObservationModRevision := int64(0)
	priorObservationRevision := uint64(0)
	predecessorTaskID := ""
	expectedPreviousArtifactSHA256 := ""
	expectedPreviousArtifactID := ""
	expectedPreviousGeneration := uint64(0)
	ownershipPlanID := task.PlanID
	ownershipGeneration := uint64(task.RenderGeneration)
	composeArtifactID := ids.New(ids.KindConfig)
	if observationFound {
		priorObservationModRevision = observation.Revision
		priorObservationRevision = observation.Record.Revision
		predecessorTaskID = observation.Record.TaskID
		expectedPreviousArtifactSHA256 = observation.Record.CorefileSHA256
		if observation.Record.Enabled {
			expectedPreviousArtifactID = observation.Record.DNSResolverProof.ArtifactID
			expectedPreviousGeneration = observation.Record.DNSResolverProof.RenderGeneration
		}
	}
	if ensureService && !observationFound && len(current.Record.Runtime.GeneratedServices) != 0 {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"platform Component serving predecessor observation is unavailable",
		)
	}
	if !ensureService {
		if !observationFound || !observation.Record.Enabled || !observation.Record.Healthy ||
			observation.Record.ServiceID != generatedServiceID {
			return etcd.PlatformComponentTaskRenderInput{}, errs.New(
				errs.KindStateConflict,
				"platform Component runtime observation is unavailable",
			)
		}
		ownershipPlanID = observation.Record.PlanID
		ownershipGeneration = observation.Record.OwnershipGeneration
		composeArtifactID = observation.Record.ComposeArtifactID
	}
	composeArtifact, err := composerender.RenderPlatformComponentCompose(
		composerender.PlatformComponentComposeInput{
			ComponentID: desired.ID, PlanID: ownershipPlanID, RenderGeneration: ownershipGeneration,
			ArtifactID: composeArtifactID, Plan: selectedPlan,
			ImageRepository: image.Repository, ImageIndexDigest: image.IndexDigest,
			ImageConfigDigest: selectedPlatform.ConfigDigest,
			ImageChildDigest:  selectedPlatform.ChildDigest, ImageReference: selectedReference,
			ImageOS: selectedPlatform.OS, ImageArchitecture: selectedPlatform.Architecture,
			ImageVariant: selectedPlatform.Variant,
		},
	)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	var rollbackComposeArtifact *agentpb.ComposeArtifact
	if ensureService && observationFound && observation.Record.Enabled {
		if observation.Record.ComposeArtifact == nil {
			return etcd.PlatformComponentTaskRenderInput{}, errs.New(
				errs.KindStateConflict,
				"platform Component serving predecessor artifact is unavailable",
			)
		}
		rollbackComposeArtifact = proto.Clone(observation.Record.ComposeArtifact).(*agentpb.ComposeArtifact)
	}
	input := etcd.PlatformComponentTaskRenderInput{
		PlanID: task.PlanID, TaskID: task.ID, ComponentID: desired.ID,
		DesiredSHA256:      desiredSHA256,
		BaselineGeneration: baseline.Record.Generation, BaselineSHA256: baseline.Record.SHA256,
		HostResolutionInputRevision: hostResolution.Record.InputRevision,
		HostResolutionSHA256:        hostResolution.Record.InputSHA256,
		Config:                      *config, Hosts: durableHosts, GeneratedServiceID: generatedServiceID,
		EnsureService: ensureService, DisableService: disableService,
		DefinitionSHA256: hex.EncodeToString(definitionDigest[:]),
		CatalogSHA256:    hex.EncodeToString(catalogDigest[:]), ActionID: string(action.ID()),
		ArtifactID: ids.New(ids.KindConfig), ComposeArtifactID: composeArtifactID,
		ComposeArtifact: composeArtifact, RollbackComposeArtifact: rollbackComposeArtifact,
		OwnershipPlanID: ownershipPlanID, OwnershipGeneration: ownershipGeneration,
		PriorObservationModRevision:    priorObservationModRevision,
		PriorObservationRevision:       priorObservationRevision,
		PredecessorTaskID:              predecessorTaskID,
		ExpectedPreviousArtifactSHA256: expectedPreviousArtifactSHA256,
		ExpectedPreviousArtifactID:     expectedPreviousArtifactID,
		ExpectedPreviousGeneration:     expectedPreviousGeneration,
		ImageRepository:                image.Repository, ImageIndexDigest: image.IndexDigest,
		ImageConfigDigest: selectedPlatform.ConfigDigest,
		ImageOS:           selectedPlatform.OS, ImageArchitecture: selectedPlatform.Architecture,
		ImageVariant: selectedPlatform.Variant, ImageChildDigest: selectedPlatform.ChildDigest,
		ImageReference: selectedReference,
		ArtifactSHA256: hex.EncodeToString(intent.ArtifactSHA256[:]),
		ArtifactLength: intent.ArtifactLength,
		PlanSHA256:     hex.EncodeToString(intent.PlanSHA256[:]),
	}
	return sealPlatformComponentTaskPlanHash(task, input, selectedPlan.Services[0].ObservationAction)
}

func sealPlatformComponentTaskPlanHash(
	task etcd.TaskRecord,
	input etcd.PlatformComponentTaskRenderInput,
	observationAction componentsdk.ActionID,
) (etcd.PlatformComponentTaskRenderInput, error) {
	definitionDigest, err := componentDigest(input.DefinitionSHA256)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	catalogDigest, err := componentDigest(input.CatalogSHA256)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	artifactDigest, err := componentDigest(input.ArtifactSHA256)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	componentID, err := componentsdk.NewComponentID(input.ComponentID)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	artifactID, err := componentsdk.NewArtifactID(input.ArtifactID)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	artifact, err := componentsdk.NewArtifactReference(artifactID, artifactDigest)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	envelope, err := componentsdk.NewActionEnvelope(componentsdk.ActionEnvelopeInput{
		ComponentID: componentID, DefinitionDigest: definitionDigest, CatalogDigest: catalogDigest,
		ActionID: componentsdk.ActionID(input.ActionID), Artifact: artifact,
		Generation: uint64(task.RenderGeneration),
	})
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, errs.Wrap(errs.KindInternal, err)
	}
	previousArtifactDigest, err := hex.DecodeString(input.ExpectedPreviousArtifactSHA256)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindInternal,
			"platform Component prior artifact digest is invalid",
		)
	}
	stepIDs := make([]string, len(task.Steps))
	for index, step := range task.Steps {
		stepIDs[index] = step.ID
	}
	var execution *agentpb.ExecutionPlan
	if input.DisableService {
		execution, err = taskplan.BuildComponentDisable(taskplan.ComponentDisableInput{
			VolumeRoot: environmentpath.DefaultVolumeRoot, Envelope: envelope, PlanID: task.PlanID,
			StepIDs: stepIDs, RenderGeneration: uint64(task.RenderGeneration),
			ComposeArtifact: input.ComposeArtifact, ObservationAction: observationAction,
			ExpectedPreviousArtifactDigest: previousArtifactDigest,
		})
	} else {
		execution, err = taskplan.BuildComponentAction(taskplan.ComponentActionInput{
			VolumeRoot: environmentpath.DefaultVolumeRoot, Envelope: envelope, PlanID: task.PlanID,
			StepIDs: stepIDs, RenderGeneration: uint64(task.RenderGeneration),
			ComposeArtifact: input.ComposeArtifact, RollbackComposeArtifact: input.RollbackComposeArtifact,
			EnsureService: input.EnsureService, ObservationAction: observationAction,
			ExpectedPreviousArtifactDigest: previousArtifactDigest,
			ExpectedPreviousArtifactID:     input.ExpectedPreviousArtifactID,
			ExpectedPreviousGeneration:     input.ExpectedPreviousGeneration,
		})
	}
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	input.ExecutionPlanSHA256 = hex.EncodeToString(execution.GetPlanHash())
	return input, nil
}

func componentTaskEnsureService(task etcd.TaskRecord) (bool, error) {
	switch len(task.Steps) {
	case 2:
		return false, nil
	case 4:
		return true, nil
	default:
		return false, errs.New(errs.KindInternal, "platform Component Task procedure is invalid")
	}
}

// SelectResolver selects the sole platform-owned Component after proving that
// the compiled catalog contains a resolver definition with its generic grants.
func (planner *PlatformRenderPlanner) SelectResolver(
	ctx context.Context,
	candidates []etcdstore.Versioned[componentrecord.Record],
) (etcdstore.Versioned[componentrecord.Record], error) {
	if ctx == nil || planner == nil || planner.catalog == nil {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindInternal,
			"dns-resolver selector dependencies are required",
		)
	}
	definition, _, found := planner.catalog.FindActionByCapability(
		componentsdk.CapabilityDNSResolver, planner.managedConfigAction,
	)
	if !found || !definitionProvidesResolverGrants(definition) {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindInternal,
			"registered dns-resolver capability is absent from the compiled catalog",
		)
	}
	var selected etcdstore.Versioned[componentrecord.Record]
	for _, candidate := range candidates {
		if candidate.Record.Desired.Owner != core.ComponentOwnerPlatform || candidate.Record.Desired.OwnerID != "" {
			continue
		}
		if selected.Revision != 0 {
			return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
				errs.KindStateConflict,
				"multiple platform dns-resolver Components are registered",
			)
		}
		selected = candidate
	}
	if selected.Revision <= 0 || selected.ReadRevision <= 0 {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindComponentNotFound,
			"platform dns-resolver Component is not registered",
		)
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
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task etcd.TaskRecord,
) (etcd.PlatformComponentTaskRenderInput, error) {
	currentComponent, err := componentrecord.ProjectRecord(current.Record)
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
	input, err := planner.prepareConfigTask(ctx, current, currentComponent, task, true)
	if err != nil {
		return etcd.PlatformComponentTaskRenderInput{}, err
	}
	replacement, err := componentrecord.ReplaceDesired(current.Record, desired)
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
		return componentdns.ResolverInput{}, nil, errs.New(
			errs.KindInternal,
			"host-resolution projection digest is corrupt",
		)
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
		durable[index] = etcd.PlatformDNSHost{
			Address:   host.Address.String(),
			Hostnames: append([]string(nil), host.Hostnames...),
		}
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
