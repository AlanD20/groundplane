package dnsresolver

import (
	"context"
	"crypto/sha256"
	"math"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type WorkRequest struct {
	TaskID, WorkID, PlanID, OperationID, HostID string
	ImageOS, ImageArchitecture                  string
	Actor                                       workexecution.Actor
	Action                                      workexecution.Action
	CreatedAt, Deadline                         time.Time
	EnsureRuntime                               bool
}

type PreparedWork struct {
	Input             resolverexecution.Input
	Capture           resolverwork.Capture
	Desired           componentrecord.Record
	InitialProjection bool
}

// PrepareWork captures the compiled DNS recipe directly. It never constructs a
// TaskRecord, a hidden Task or an ExecutionPlan to obtain native host authority.
func (planner *PlatformRenderPlanner) PrepareWork(ctx context.Context, records *resolverwork.Repository,
	current keyvalue.Versioned[componentrecord.Record], desired core.Component, request WorkRequest,
) (PreparedWork, error) {
	if ctx == nil || records == nil || current.Revision <= 0 || current.ReadRevision < current.Revision {
		return PreparedWork{}, errs.New(errs.KindInternal, "DNS resolver planning requires a retained Component and native storage")
	}
	if err := ValidateComponent(planner.renderer, desired); err != nil {
		return PreparedWork{}, err
	}
	replacement, err := componentrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return PreparedWork{}, err
	}
	if err := ensureHostResolverBaseline(ctx, planner.baselines, planner.capture, request.CreatedAt); err != nil {
		return PreparedWork{}, err
	}
	baseline, found, err := planner.baselines.GetHostResolverBaseline(ctx)
	if err != nil {
		return PreparedWork{}, err
	}
	if !found {
		return PreparedWork{}, invalid("host resolver baseline is not initialized")
	}
	projection, found, err := planner.projections.GetHostResolutionProjection(ctx)
	if err != nil {
		return PreparedWork{}, err
	}
	initialProjection := !found
	readRevision := max(current.ReadRevision, baseline.ReadRevision, projection.ReadRevision)
	if initialProjection {
		projection.Record, err = hostresolution.NewHostResolutionProjectionRecord(readRevision, nil)
		if err != nil {
			return PreparedWork{}, err
		}
	}
	applied, hasApplied, err := records.ReadApplied(ctx, request.HostID, desired.ID, readRevision)
	if err != nil {
		return PreparedWork{}, err
	}
	if !hasApplied && len(current.Record.Runtime.GeneratedServices) != 0 {
		return PreparedWork{}, errs.New(errs.KindStateConflict, "CoreDNS has runtime identities but no captured applied configuration; reconcile the installation before changing it")
	}
	intentRevision, err := records.ReadIntent(ctx, request.HostID, desired.ID, readRevision)
	if err != nil {
		return PreparedWork{}, err
	}
	generation := uint64(1)
	var previous *resolverexecution.Configuration
	if hasApplied {
		if applied.Record.Generation == math.MaxUint64 {
			return PreparedWork{}, invalid("DNS resolver configuration generation is exhausted")
		}
		generation, previous = applied.Record.Generation+1, applied.Record.Configuration
	}
	input := resolverexecution.Input{TaskID: request.TaskID, WorkID: request.WorkID, HostID: request.HostID,
		BaselineSHA256: baseline.Record.SHA256,
		OperationID:    request.OperationID, ComponentID: desired.ID, Generation: generation, CreatedAt: request.CreatedAt.UTC(),
		Deadline: request.Deadline.UTC(), Enabled: desired.Enabled, Previous: previous}
	if hasApplied {
		input.PreviousEnabled, input.PreviousFileOwner = applied.Record.Desired.Enabled, applied.Record.FileOwner
	}
	capture := resolverwork.Capture{PlanID: request.PlanID, TaskID: request.TaskID, WorkID: request.WorkID, HostID: request.HostID,
		TargetName: "CoreDNS", Actor: request.Actor, Action: request.Action, Desired: replacement.Desired, ReadRevision: readRevision,
		ComponentRevision: current.Revision, AppliedRevision: applied.Revision, IntentRevision: intentRevision, BaselineRevision: baseline.Revision,
		BaselineSHA256: baseline.Record.SHA256, ProjectionRevision: projection.Revision, Projection: projection.Record}
	if desired.Enabled {
		resolvers, err := componentdns.ParseResolverBaseline(baseline.Record.Content)
		if err != nil {
			return PreparedWork{}, err
		}
		selected, _, err := resolverInputFromProjection(projection.Record, baseline.Record.Generation, resolvers, planner.registryAddress)
		if err != nil {
			return PreparedWork{}, err
		}
		hosts, guards, environmentDNS, err := resolveManagedRecords(ctx, planner.records, desired.Config.CoreDNS.Records, readRevision)
		if err != nil {
			return PreparedWork{}, err
		}
		selected.HostResolution, capture.Hosts, err = mergeResolverHosts(selected.HostResolution, hosts)
		if err != nil {
			return PreparedWork{}, err
		}
		listeners, err := planner.zoneListeners.Resolve(ctx, readRevision)
		if err != nil {
			return PreparedWork{}, err
		}
		selected.ZoneListeners, err = platformcomponents.ParseZoneListeners(listeners.Addresses)
		if err != nil {
			return PreparedWork{}, err
		}
		capture.References, err = mergeResolverConditions(guards, listeners.Conditions)
		if err != nil {
			return PreparedWork{}, err
		}
		capture.EnvironmentDNS = environmentDNS
		if selected.PrivateListener.IsValid() {
			capture.PrivateListener = selected.PrivateListener.String()
		}
		for _, address := range selected.ZoneListeners {
			capture.ZoneListeners = append(capture.ZoneListeners, address.String())
		}
		request.EnsureRuntime = request.EnsureRuntime || !input.PreviousEnabled
		candidate, ensure, err := planner.prepareNativeConfiguration(desired, selected, request, generation, previous)
		if err != nil {
			return PreparedWork{}, err
		}
		input.Candidate, input.EnsureRuntime = &candidate, ensure
	}
	if err := capture.Validate(input); err != nil {
		return PreparedWork{}, err
	}
	return PreparedWork{Input: input, Capture: capture, Desired: replacement, InitialProjection: initialProjection}, nil
}

func (planner *PlatformRenderPlanner) prepareNativeConfiguration(desired core.Component, selected componentdns.ResolverInput,
	request WorkRequest, generation uint64, previous *resolverexecution.Configuration,
) (resolverexecution.Configuration, bool, error) {
	definition, action, found := planner.catalog.FindActionByCapability(componentsdk.CapabilityDNSResolver, planner.managedConfigAction)
	if !found || !definitionProvidesResolverGrants(definition) {
		return resolverexecution.Configuration{}, false, invalid("compiled DNS resolver capability is unavailable")
	}
	config, err := DecodeConfig(desired.Config)
	if err != nil {
		return resolverexecution.Configuration{}, false, err
	}
	renderInput, err := BuildRenderInput(selected.HostResolution.Hosts, config, selected.Baseline.Resolvers, selected.PrivateListener, selected.ZoneListeners)
	if err != nil {
		return resolverexecution.Configuration{}, false, err
	}
	serviceID := ids.New(ids.KindService)
	if previous != nil {
		serviceID = previous.Runtime.Services[0].ServiceId
	}
	plan, err := planner.environmentPlanner.Plan(definition.Implementation(), serviceID, renderInput)
	if err != nil {
		return resolverexecution.Configuration{}, false, err
	}
	defer clearEnvironmentPlan(plan)
	if len(plan.Services) != 1 || len(plan.Files) != 1 || len(plan.Files[0].Content) == 0 {
		return resolverexecution.Configuration{}, false, invalid("compiled DNS resolver must produce one Service and one Corefile")
	}
	image := plan.Services[0].Image
	platform, reference, supported := image.Select(request.ImageOS, request.ImageArchitecture)
	if !supported || request.ImageOS != "linux" {
		return resolverexecution.Configuration{}, false, invalid("DNS resolver image is unavailable for the selected host platform")
	}
	ensure := request.EnsureRuntime || previous == nil || previous.Runtime.Services[0].ImageReference != reference
	configuration := resolverexecution.Configuration{PlanID: request.PlanID, Generation: generation, RuntimeGeneration: generation,
		Corefile: append([]byte(nil), plan.Files[0].Content...)}
	if ensure {
		configuration.Runtime, err = composerender.RenderPlatformComponentCompose(composerender.PlatformComponentComposeInput{
			ComponentID: desired.ID, PlanID: request.PlanID, RenderGeneration: generation, ArtifactID: ids.New(ids.KindConfig), Plan: plan,
			ImageRepository: image.Repository, ImageIndexDigest: image.IndexDigest, ImageConfigDigest: platform.ConfigDigest,
			ImageChildDigest: platform.ChildDigest, ImageReference: reference, ImageOS: platform.OS, ImageArchitecture: platform.Architecture, ImageVariant: platform.Variant})
		if err != nil {
			return resolverexecution.Configuration{}, false, err
		}
	} else {
		configuration.PlanID, configuration.RuntimeGeneration = previous.PlanID, previous.RuntimeGeneration
		configuration.Runtime = proto.CloneOf(previous.Runtime)
	}
	definitionDigest, catalogDigest := definition.Digest(), planner.catalog.Digest()
	digest := sha256.Sum256(configuration.Corefile)
	artifactDigest := digest[:]
	configuration.Apply = &agentpb.ComponentApply{ComponentId: desired.ID, DefinitionDigest: append([]byte(nil), definitionDigest[:]...),
		CatalogDigest: append([]byte(nil), catalogDigest[:]...), ActionId: string(action.ID()), ArtifactId: ids.New(ids.KindConfig),
		ArtifactDigest: artifactDigest, Generation: generation, ManagedConfigContent: true}
	if previous != nil {
		configuration.Apply.ExpectedPreviousArtifactDigest = append([]byte(nil), previous.Apply.ArtifactDigest...)
		configuration.Apply.ExpectedPreviousArtifactId, configuration.Apply.ExpectedPreviousGeneration = previous.Apply.ArtifactId, previous.Generation
	}
	configuration.Observe = &agentpb.ComponentApply{ComponentId: desired.ID, DefinitionDigest: append([]byte(nil), definitionDigest[:]...),
		CatalogDigest: append([]byte(nil), catalogDigest[:]...), ActionId: string(plan.Services[0].ObservationAction),
		ArtifactId: configuration.Apply.ArtifactId, ArtifactDigest: append([]byte(nil), artifactDigest...), Generation: generation}
	return configuration, ensure, configuration.Validate(desired.ID)
}
