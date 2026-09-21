package backingservices

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/compose-spec/compose-go/v2/loader"
	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	testtaskcontract "github.com/AlanD20/groundplane/internal/controller/taskcontract"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type customBackingCreationRepository struct {
	*etcd.EnvironmentBlueprintRepository
	desired     *desiredrevisionstore.Repository
	publication etcd.BackingServiceCreation
}

func (repository *customBackingCreationRepository) ClaimEnvironmentBlueprintStage(
	ctx context.Context,
	request testblueprints.EnvironmentBlueprintStageClaimRequest,
) (testblueprints.EnvironmentBlueprintStageClaim, error) {
	return repository.desired.ClaimEnvironmentBlueprintStage(ctx, request)
}

func (repository *customBackingCreationRepository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request testblueprints.EnvironmentBlueprintStageRequest,
) (testblueprints.EnvironmentBlueprintSeal, error) {
	return repository.desired.StageEnvironmentBlueprintRevision(ctx, request)
}

func (repository *customBackingCreationRepository) PublishBackingServiceWithTask(
	ctx context.Context,
	creation etcd.BackingServiceCreation,
) (etcd.IdempotencyTransactionResult, error) {
	repository.publication = creation
	return repository.EnvironmentBlueprintRepository.PublishBackingServiceWithTask(ctx, creation)
}

// QA: BACK-11.
// Rationale: custom creation must publish and reconstruct the exact operator
// image as a network-only Compose workload without managed backing defaults.
func TestCustomBackingServiceCreationPublishesNetworkOnlyRuntime(t *testing.T) {
	registerAdapters()
	store := newBackingCreationStore()
	blueprints, err := etcd.NewEnvironmentBlueprintRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	desiredStore, err := desiredrevisionstore.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	repository := &customBackingCreationRepository{
		EnvironmentBlueprintRepository: blueprints,
		desired:                        desiredStore,
	}
	protector, err := secretvalue.NewProtector(backingTestCrypt{}, backingTestCrypt{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	idempotencyRecords, err := etcd.NewIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := desiredrevision.NewIdempotency(coordinator, idempotencyRecords)
	if err != nil {
		t.Fatal(err)
	}
	const volumeRoot = "/var/lib/groundplane/volumes"
	service, err := NewCreationService(
		volumeRoot,
		netip.MustParsePrefix("10.0.0.0/8"),
		repository,
		idempotency,
		protector,
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC) }

	const image = "registry.example/internal/search:1.4"
	response, err := service.CreateBackingService(t.Context(), apiTypes.BackingServiceCreate{
		Slug: "search-api", Name: "Search API", Adapter: "custom", Image: image,
		NetworkPool: "10.200.0.0/24",
		Zone: apiTypes.BackingServiceZoneCreate{
			Name: "data", Subnet: "10.200.0.0/28", Internal: true,
		},
	}, "custom-create-network-only")
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != 201 {
		t.Fatalf("custom creation status = %d", response.Status)
	}

	creation := repository.publication
	desired := creation.Service.Desired
	if desired.Name != "search-api" || desired.Image != image || desired.Adapter != "custom" ||
		len(desired.Mounts) != 0 || len(desired.Command) != 0 || len(desired.Expose) != 0 ||
		desired.Healthcheck.HTTP != "" || desired.Healthcheck.TCP != "" || desired.Healthcheck.Pgrep != "" {
		t.Fatalf("custom desired Service = %#v", desired)
	}
	if len(creation.Projection.Volumes) != 0 || len(creation.Projection.VolumeMounts) != 0 ||
		len(creation.Projection.Entries) != 0 || len(creation.Entries) != 0 || len(creation.EntryValues) != 0 ||
		len(creation.Secrets) != 0 || len(creation.SecretValues) != 0 {
		t.Fatalf("custom managed state = %#v", creation.Projection)
	}
	if creation.Task.Params[testtaskjournal.TaskBackingServiceCreationParam] != desired.ID ||
		creation.Task.Params[testtaskjournal.TaskBackingServiceHealthParam] != "" ||
		creation.Task.Params[testtaskcontract.EnvironmentBlueprintManagedVolumesParam] != "" ||
		len(creation.Task.Materializations) != 0 || len(creation.Task.Steps) != 2 {
		t.Fatalf("custom creation Task = %#v", creation.Task)
	}

	resolver, err := testtaskplanning.NewTaskPlanResolverWithBlueprints(volumeRoot, repository, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveExecutionPlan(t.Context(), creation.Task)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE ||
		len(plan.Steps) != 2 || plan.Steps[0].GetEnvironmentDirectoryCreate() == nil ||
		plan.Steps[1].GetComposeApply() == nil || !plan.Steps[1].GetComposeApply().FullReconcile ||
		len(plan.Artifacts) != 1 || len(plan.Artifacts[0].Services) != 1 {
		t.Fatalf("custom creation plan = %#v", plan)
	}
	artifactService := plan.Artifacts[0].Services[0]
	if artifactService.ComposeName != "search-api" || artifactService.HasHealthcheck {
		t.Fatalf("custom Compose service identity = %#v", artifactService)
	}
	parsed, err := loader.LoadWithContext(t.Context(), composetypes.ConfigDetails{
		WorkingDir: creation.Environment.VolumeDir,
		ConfigFiles: []composetypes.ConfigFile{{
			Filename: "compose.yaml", Content: plan.Artifacts[0].CanonicalYaml,
		}},
	}, func(options *loader.Options) { options.SkipResolveEnvironment = true })
	if err != nil {
		t.Fatal(err)
	}
	runtime, exists := parsed.Services["search-api"]
	if !exists || runtime.Image != image || len(runtime.Networks) != 1 || runtime.Networks["data"] == nil ||
		len(runtime.Volumes) != 0 || len(runtime.Environment) != 0 || len(runtime.EnvFiles) != 0 ||
		len(runtime.Command) != 0 || len(runtime.Expose) != 0 || runtime.HealthCheck != nil ||
		len(parsed.Volumes) != 0 {
		t.Fatalf("custom parsed Compose runtime = %#v", runtime)
	}
}
