package app

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
	registeredcoredns "github.com/AlanD20/groundplane-registered-components/coredns"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	controllerdns "github.com/AlanD20/groundplane/internal/controller/dnsresolver"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type platformComponentExecutionPlanner struct {
	components *etcd.ComponentRepository
	catalog    registeredActionCatalog
	renderer   componentdns.Renderer
}

type resolvedPlatformComponent struct {
	input    etcd.PlatformComponentTaskRenderInput
	plan     componentsdk.EnvironmentPlan
	envelope componentsdk.ActionEnvelope
}

func newPlatformComponentExecutionPlanner(
	components *etcd.ComponentRepository,
	catalog registeredActionCatalog,
	renderer componentdns.Renderer,
) (*platformComponentExecutionPlanner, error) {
	if components == nil || renderer == nil {
		return nil, errs.New(errs.KindInternal, "Platform Component execution planner dependencies are required")
	}
	return &platformComponentExecutionPlanner{components: components, catalog: catalog, renderer: renderer}, nil
}

func (planner *platformComponentExecutionPlanner) ResolveComponentExecutionPlan(
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
	var composeArtifact *agentpb.ComposeArtifact
	if resolved.input.EnsureService || resolved.input.DisableService {
		composeArtifact, err = controllerpkg.RenderPlatformComponentCompose(
			controllerpkg.PlatformComponentComposeInput{
				ComponentID: task.Target, PlanID: task.PlanID,
				RenderGeneration: uint64(task.RenderGeneration),
				ArtifactID: resolved.input.ComposeArtifactID, Plan: resolved.plan,
			},
		)
		if err != nil {
			return nil, err
		}
	}
	if resolved.input.DisableService {
		return controllerpkg.BuildComponentDisableExecutionPlan(
			task.Target, task.PlanID, stepIDs, uint64(task.RenderGeneration), composeArtifact,
		)
	}
	return controllerpkg.BuildComponentActionExecutionPlan(controllerpkg.ComponentActionPlanInput{
		Envelope: resolved.envelope, PlanID: task.PlanID, StepIDs: stepIDs,
		RenderGeneration: uint64(task.RenderGeneration),
		ComposeArtifact: composeArtifact, EnsureService: resolved.input.EnsureService,
	})
}

func (planner *platformComponentExecutionPlanner) ResolveManagedConfig(
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
		subtle.ConstantTimeCompare(action.GetArtifactDigest(), artifactDigest[:]) != 1 {
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
		Length: uint64(len(content)), Content: newOwnedComponentArtifact(content),
	}, nil
}

func (planner *platformComponentExecutionPlanner) resolve(
	ctx context.Context,
	task etcd.TaskRecord,
) (resolvedPlatformComponent, error) {
	if ctx == nil || task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskUpdate ||
		ids.Validate(ids.KindComponent, task.Target) != nil || ids.Validate(ids.KindPlan, task.PlanID) != nil ||
		task.RenderGeneration <= 0 || task.Params[etcd.TaskResourceKindParam] != etcd.TaskResourceComponent ||
		len(task.Params) != 2 {
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "Platform Component Task shape is invalid")
	}
	stored, err := planner.components.GetPlatformComponentTaskRenderInput(ctx, task.PlanID)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	input := stored.Record
	expectedSteps := 1
	if input.EnsureService {
		expectedSteps = 4
	} else if input.DisableService {
		expectedSteps = 2
	}
	if input.TaskID != task.ID || input.ComponentID != task.Target || len(task.Steps) != expectedSteps ||
		input.DesiredSHA256 != task.Params[etcd.TaskPlatformComponentDesiredSHA256Param] {
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "Platform Component render input does not match its Task")
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
		return resolvedPlatformComponent{}, errs.New(errs.KindStateConflict, "Platform Component Task desired state was superseded")
	}
	baseline, found, err := planner.components.GetHostResolverBaseline(ctx)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	if !found || baseline.Record.Generation != input.BaselineGeneration || baseline.Record.SHA256 != input.BaselineSHA256 {
		return resolvedPlatformComponent{}, errs.New(errs.KindStateConflict, "host resolver baseline does not match the Component Task")
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
	config, err := controllerdns.DecodeConfig(core.ComponentConfig{CoreDNS: &input.Config})
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	renderInput, err := controllerdns.BuildRenderInput(hosts, config, baselineResolvers)
	if err != nil {
		return resolvedPlatformComponent{}, err
	}
	registeredPlan, err := registeredcoredns.Plan(registeredcoredns.PlanInput{
		GeneratedServiceID: input.GeneratedServiceID, Render: renderInput,
	})
	if err != nil {
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	if len(registeredPlan.Files) != 1 || uint64(len(registeredPlan.Files[0].Content)) != input.ArtifactLength {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "registered Component artifact length changed")
	}
	artifactDigest := sha256.Sum256(registeredPlan.Files[0].Content)
	expectedArtifactDigest, err := componentDigest(input.ArtifactSHA256)
	if err != nil || subtle.ConstantTimeCompare(artifactDigest[:], expectedArtifactDigest[:]) != 1 {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.New(errs.KindInternal, "registered Component artifact digest changed")
	}
	definitionDigest, err := componentDigest(input.DefinitionSHA256)
	if err != nil {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, err
	}
	catalogDigest, err := componentDigest(input.CatalogSHA256)
	if err != nil {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, err
	}
	componentID, err := componentsdk.NewComponentID(input.ComponentID)
	if err != nil {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	artifactID, err := componentsdk.NewArtifactID(input.ArtifactID)
	if err != nil {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	artifact, err := componentsdk.NewArtifactReference(artifactID, expectedArtifactDigest)
	if err != nil {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	envelope, err := componentsdk.NewActionEnvelope(componentsdk.ActionEnvelopeInput{
		ComponentID: componentID, DefinitionDigest: definitionDigest, CatalogDigest: catalogDigest,
		ActionID: componentsdk.ActionID(input.ActionID), Artifact: artifact,
		Generation: uint64(task.RenderGeneration),
	})
	if err != nil {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.Wrap(errs.KindInternal, err)
	}
	definition, action, err := planner.catalog.ResolveActionEnvelope(envelope)
	if err != nil || definition.Implementation() != componentsdk.ImplementationKey(core.ComponentKindCoreDNS) ||
		action.ID() != coreDNSActivateConfigAction {
		clearPlatformComponentPlan(registeredPlan)
		return resolvedPlatformComponent{}, errs.New(errs.KindStateConflict, "Component action is not present in the compiled catalog")
	}
	return resolvedPlatformComponent{input: input, plan: registeredPlan, envelope: envelope}, nil
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
