package dnsresolver

import (
	"context"
	"encoding/hex"
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	resolverbaseline "github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"runtime"
	"time"
)

type PlatformProjectionReader interface {
	GetHostResolutionProjection(context.Context) (etcdstore.Versioned[resolutionrecord.HostResolutionProjectionRecord], bool, error)
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
