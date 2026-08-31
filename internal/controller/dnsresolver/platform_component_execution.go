package dnsresolver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/netip"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ExecutionCatalog interface {
	ResolveActionEnvelope(componentsdk.ActionEnvelope) (componentsdk.Definition, componentsdk.ActionDefinition, error)
	Plan(componentsdk.ImplementationKey, string, componentdns.RenderInput) (componentsdk.EnvironmentPlan, error)
}

type ComponentExecutionRepository interface {
	GetPlatformComponentTaskRenderInput(
		context.Context,
		string,
	) (etcd.Versioned[etcd.PlatformComponentTaskRenderInput], error)
	GetComponent(context.Context, string) (etcd.Versioned[etcd.ComponentRecord], error)
	GetHostResolverBaseline(context.Context) (etcd.Versioned[etcd.HostResolverBaselineRecord], bool, error)
}

type PlatformExecutionPlanner struct {
	volumeRoot string
	components ComponentExecutionRepository
	catalog    ExecutionCatalog
}

type resolvedPlatformComponent struct {
	input          etcd.PlatformComponentTaskRenderInput
	plan           componentsdk.EnvironmentPlan
	envelope       componentsdk.ActionEnvelope
	implementation componentsdk.ImplementationKey
}

func NewPlatformComponentExecutionPlanner(
	volumeRoot string,
	components ComponentExecutionRepository,
	catalog ExecutionCatalog,
) (*PlatformExecutionPlanner, error) {
	if volumeRoot == "" || components == nil || catalog == nil {
		return nil, errs.New(errs.KindInternal, "Platform Component execution planner dependencies are required")
	}
	return &PlatformExecutionPlanner{volumeRoot: volumeRoot, components: components, catalog: catalog}, nil
}

func (planner *PlatformExecutionPlanner) ResolveComponentExecutionPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	resolved, err := planner.resolve(ctx, task)
	if err != nil {
		return nil, err
	}
	defer clearPlatformComponentPlan(resolved.plan)
	stepIDs := make([]string, len(task.Steps))
	for index, step := range task.Steps {
		stepIDs[index] = step.ID
	}
	service := resolved.plan.Services[0]
	ownershipPlanID := task.PlanID
	ownershipGeneration := uint64(task.RenderGeneration)
	if !resolved.input.EnsureService && !resolved.input.DisableService {
		ownershipPlanID = resolved.input.OwnershipPlanID
		ownershipGeneration = resolved.input.OwnershipGeneration
	}
	composeArtifact, err := controllerpkg.RenderPlatformComponentCompose(
		controllerpkg.PlatformComponentComposeInput{
			ComponentID: task.Target, PlanID: ownershipPlanID,
			RenderGeneration: ownershipGeneration,
			ArtifactID:       resolved.input.ComposeArtifactID, Plan: resolved.plan,
			ImageRepository: resolved.input.ImageRepository, ImageIndexDigest: resolved.input.ImageIndexDigest,
			ImageChildDigest: resolved.input.ImageChildDigest, ImageReference: resolved.input.ImageReference,
			ImageOS: resolved.input.ImageOS, ImageArchitecture: resolved.input.ImageArchitecture,
			ImageVariant: resolved.input.ImageVariant,
		},
	)
	if err != nil {
		return nil, err
	}
	if resolved.input.ComposeArtifact != nil && !proto.Equal(composeArtifact, resolved.input.ComposeArtifact) {
		return nil, errs.New(errs.KindStateConflict, "platform Component Compose artifact changed after publication")
	}
	var execution *agentpb.ExecutionPlan
	if resolved.input.DisableService {
		expectedPreviousArtifactDigest, decodeErr := hex.DecodeString(
			resolved.input.ExpectedPreviousArtifactSHA256,
		)
		if decodeErr != nil {
			return nil, errs.New(errs.KindInternal, "platform Component prior artifact digest is invalid")
		}
		execution, err = controllerpkg.BuildComponentDisableExecutionPlan(controllerpkg.ComponentDisablePlanInput{
			VolumeRoot: planner.volumeRoot, Envelope: resolved.envelope, PlanID: task.PlanID,
			StepIDs: stepIDs, RenderGeneration: uint64(task.RenderGeneration), ComposeArtifact: composeArtifact,
			ObservationAction:              service.ObservationAction,
			ExpectedPreviousArtifactDigest: expectedPreviousArtifactDigest,
		})
	} else {
		expectedPreviousArtifactDigest, decodeErr := hex.DecodeString(
			resolved.input.ExpectedPreviousArtifactSHA256,
		)
		if decodeErr != nil {
			return nil, errs.New(errs.KindInternal, "platform Component prior artifact digest is invalid")
		}
		execution, err = controllerpkg.BuildComponentActionExecutionPlan(controllerpkg.ComponentActionPlanInput{
			VolumeRoot: planner.volumeRoot,
			Envelope:   resolved.envelope, PlanID: task.PlanID, StepIDs: stepIDs,
			RenderGeneration: uint64(task.RenderGeneration),
			ComposeArtifact:  composeArtifact, EnsureService: resolved.input.EnsureService,
			RollbackComposeArtifact:        resolved.input.RollbackComposeArtifact,
			ObservationAction:              service.ObservationAction,
			ExpectedPreviousArtifactDigest: expectedPreviousArtifactDigest,
			ExpectedPreviousArtifactID:     resolved.input.ExpectedPreviousArtifactID,
			ExpectedPreviousGeneration:     resolved.input.ExpectedPreviousGeneration,
		})
	}
	if err != nil {
		return nil, err
	}
	if hex.EncodeToString(execution.GetPlanHash()) != task.PlanHash {
		return nil, errs.New(errs.KindStateConflict, "registered Component execution plan changed")
	}
	return execution, nil
}

func (planner *PlatformExecutionPlanner) ResolveManagedConfig(
	ctx context.Context,
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (agentchannel.ManagedConfigSource, error) {
	resolved, err := planner.resolve(ctx, task)
	if err != nil {
		return agentchannel.ManagedConfigSource{}, err
	}
	action := step.GetComponentApply()
	artifactDigest := resolved.envelope.Artifact().Digest()
	if plan == nil || step == nil || action == nil || task.PlanID != plan.GetPlanId() ||
		plan.GetTargetId() != task.Target || action.GetArtifactId() != resolved.input.ArtifactID ||
		len(action.GetArtifactDigest()) != sha256.Size ||
		subtle.ConstantTimeCompare(action.GetArtifactDigest(), artifactDigest[:]) != 1 ||
		hex.EncodeToString(
			action.GetExpectedPreviousArtifactDigest(),
		) != resolved.input.ExpectedPreviousArtifactSHA256 {
		clearPlatformComponentPlan(resolved.plan)
		return agentchannel.ManagedConfigSource{}, errs.New(
			errs.KindInternal,
			"managed-config source request does not match the sealed Component action",
		)
	}
	if len(resolved.plan.Files) != 1 {
		clearPlatformComponentPlan(resolved.plan)
		return agentchannel.ManagedConfigSource{}, errs.New(
			errs.KindInternal,
			"registered Component plan does not contain exactly one managed config",
		)
	}
	content := resolved.plan.Files[0].Content
	resolved.plan.Files[0].Content = nil
	clearPlatformComponentPlan(resolved.plan)
	return agentchannel.ManagedConfigSource{
		MediaType: managedconfig.MediaTypeTextUTF8,
		Length:    uint64(len(content)), Content: newOwnedComponentArtifact(content),
	}, nil
}

func (planner *PlatformExecutionPlanner) resolve(
	ctx context.Context,
	task etcd.TaskRecord,
) (resolvedPlatformComponent, error) {
	if ctx == nil || task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskUpdate ||
		ids.Validate(ids.KindComponent, task.Target) != nil || ids.Validate(ids.KindPlan, task.PlanID) != nil ||
		task.RenderGeneration <= 0 || task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceComponent ||
		!validPlatformComponentTaskParams(task) {
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "Platform Component Task shape is invalid")
	}
	stored, err := planner.components.GetPlatformComponentTaskRenderInput(ctx, task.PlanID)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	input := stored.Record
	expectedSteps := 2
	if input.EnsureService {
		expectedSteps = 4
	} else if input.DisableService {
		expectedSteps = 2
	}
	if (input.TaskID != task.ID && task.RetryOf == "") || input.ComponentID != task.Target ||
		input.PlanID != task.PlanID || input.PlanSHA256 != task.PlanHash || len(task.Steps) != expectedSteps ||
		input.DesiredSHA256 != task.Params[etcd.TaskPlatformComponentDesiredSHA256Param] {
		return resolvedPlatformComponent{}, errs.New(
			errs.KindInternal,
			"Platform Component render input does not match its Task",
		)
	}
	current, err := planner.components.GetComponent(ctx, task.Target)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	currentDigest, err := etcd.PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	if currentDigest != input.DesiredSHA256 {
		return resolvedPlatformComponent{}, errs.New(
			errs.KindStateConflict,
			"Platform Component Task desired state was superseded",
		)
	}
	baseline, found, err := planner.components.GetHostResolverBaseline(ctx)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	if !found || baseline.Record.Generation != input.BaselineGeneration ||
		baseline.Record.SHA256 != input.BaselineSHA256 {
		return resolvedPlatformComponent{}, errs.New(
			errs.KindStateConflict,
			"host resolver baseline does not match the Component Task",
		)
	}
	baselineResolvers, err := componentdns.ParseResolverBaseline(baseline.Record.Content)
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	hosts := make([]componentdns.Host, len(input.Hosts))
	for index, host := range input.Hosts {
		address, err := netip.ParseAddr(host.Address)
		if err != nil {
			return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "Platform Component DNS host is corrupt")
		}
		hosts[index] = componentdns.Host{Address: address, Hostnames: append([]string(nil), host.Hostnames...)}
	}
	baselineInput, err := componentdns.NewResolverBaseline(input.BaselineGeneration, baselineResolvers)
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	hostDigest, err := componentDigest(input.HostResolutionSHA256)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	hostInput, err := componentdns.NewHostResolutionProjection(input.HostResolutionInputRevision, hostDigest, hosts)
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	config, err := DecodeConfig(core.ComponentConfig{CoreDNS: &input.Config})
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	renderInput, err := BuildRenderInput(hostInput.Hosts, config, baselineInput.Resolvers)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	definitionDigest, err := componentDigest(input.DefinitionSHA256)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	catalogDigest, err := componentDigest(input.CatalogSHA256)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	componentID, err := componentsdk.NewComponentID(input.ComponentID)
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	artifactID, err := componentsdk.NewArtifactID(input.ArtifactID)
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	expectedArtifactDigest, err := componentDigest(input.ArtifactSHA256)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	artifact, err := componentsdk.NewArtifactReference(artifactID, expectedArtifactDigest)
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	envelope, err := componentsdk.NewActionEnvelope(componentsdk.ActionEnvelopeInput{
		ComponentID: componentID, DefinitionDigest: definitionDigest, CatalogDigest: catalogDigest,
		ActionID: componentsdk.ActionID(input.ActionID), Artifact: artifact,
		Generation: uint64(task.RenderGeneration),
	})
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	definition, _, err := planner.catalog.ResolveActionEnvelope(envelope)
	if err != nil {
		return resolvedPlatformComponent{}, errs.New(
			errs.KindStateConflict,
			"Component action is not present in the compiled catalog",
		)
	}
	registeredPlan, err := planner.catalog.Plan(definition.Implementation(), input.GeneratedServiceID, renderInput)
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	if len(registeredPlan.Services) != 1 {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "registered Component service changed")
	}
	platform, reference, found := registeredPlan.Services[0].Image.Select(
		input.ImageOS, input.ImageArchitecture, input.ImageVariant,
	)
	if !found || reference != input.ImageReference || platform.ChildDigest != input.ImageChildDigest ||
		registeredPlan.Services[0].Image.Repository != input.ImageRepository ||
		registeredPlan.Services[0].Image.IndexDigest != input.ImageIndexDigest {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.New(
			errs.KindStateConflict,
			"registered Component platform image changed",
		)
	}
	if len(registeredPlan.Files) != 1 || uint64(len(registeredPlan.Files[0].Content)) != input.ArtifactLength {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "registered Component artifact length changed")
	}
	artifactDigest := sha256.Sum256(registeredPlan.Files[0].Content)
	if subtle.ConstantTimeCompare(artifactDigest[:], expectedArtifactDigest[:]) != 1 {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "registered Component artifact digest changed")
	}
	return resolvedPlatformComponent{
		input: input, plan: registeredPlan, envelope: envelope, implementation: definition.Implementation(),
	}, nil
}

func validPlatformComponentTaskParams(task etcd.TaskRecord) bool {
	automatic, hasAutomatic := task.Params[etcd.TaskAutomaticReconcileParam]
	if hasAutomatic && automatic != "true" {
		return false
	}
	if hasAutomatic {
		return len(task.Params) == 3 &&
			(task.Actor == etcd.TaskActorSystem || task.Actor == etcd.TaskActorOperator && task.RetryOf != "")
	}
	return len(task.Params) == 2 && task.Actor == etcd.TaskActorOperator
}

func componentDigest(value string) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(digest) {
		return digest, errs.New(errs.KindInternal, "Component action digest is corrupt")
	}
	copy(digest[:], decoded)
	return digest, nil
}

func clearPlatformComponentPlan(plan componentsdk.EnvironmentPlan) {
	for index := range plan.Files {
		clear(plan.Files[index].Content)
		plan.Files[index].Content = nil
	}
}

type ownedComponentArtifact struct {
	content []byte
	reader  *bytes.Reader
}

func newOwnedComponentArtifact(content []byte) io.ReadCloser {
	owned := append([]byte(nil), content...)
	clear(content)
	return &ownedComponentArtifact{content: owned, reader: bytes.NewReader(owned)}
}

func (source *ownedComponentArtifact) Read(destination []byte) (int, error) {
	if source.reader == nil {
		return 0, io.EOF
	}
	return source.reader.Read(destination)
}

func (source *ownedComponentArtifact) Close() error {
	clear(source.content)
	source.content = nil
	source.reader = nil
	return nil
}
