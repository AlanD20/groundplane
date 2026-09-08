package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"net/http"
	"net/netip"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

const (
	environmentBlueprintRoute           = "/environments/{id}/blueprint"
	environmentBlueprintTimeoutSeconds  = int64(120)
	maximumEnvironmentBlueprintAttempts = 3
)

type environmentBlueprintRepository interface {
	GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcd.Versioned[etcd.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	FindEnvironmentVolume(
		context.Context,
		string,
	) (etcd.Versioned[etcd.EnvironmentComposeProjection], etcd.EnvironmentVolumeIdentity, error)
	ListZones(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ZoneRecord], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	ResolveBackingProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	ResolveEnvironment(context.Context, string, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	ListRoutes(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.RouteRecord], error)
	ListEntries(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.EntryRecord], error)
	BlueprintEntryValueGenerationExists(context.Context, etcd.EntryRecord) (bool, error)
	CreateBlueprintEntryValueGeneration(context.Context, etcd.EntryValueGeneration) error
	BindBlueprintEntryEnvironment(context.Context, string, string) error
	ListAttaches(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.AttachRecord], error)
	ListEnvironmentComponents(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ComponentRecord], error)
	PrepareEnvironmentComponentTask(
		context.Context,
		string,
		string,
		[]etcd.EnvironmentBlueprintZoneChange,
		[]etcd.EnvironmentComponentCandidateInput,
		time.Time,
	) (etcd.ComponentTaskPreparation, error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		etcd.EnvironmentBlueprintStageClaimRequest,
	) (etcd.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		etcd.EnvironmentBlueprintStageRequest,
	) (etcd.EnvironmentBlueprintSeal, error)
	AbandonEnvironmentBlueprintStage(context.Context, etcd.EnvironmentBlueprintStageClaim) error
	PublishEnvironmentBlueprintDesiredRevision(
		context.Context,
		netip.Prefix,
		string,
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EnvironmentRecord],
		int64,
		etcd.EnvironmentBlueprintStageClaim,
		etcd.EnvironmentDesiredRevisionIdentity,
		etcd.EnvironmentComposeProjection,
		[]etcd.EnvironmentBlueprintZoneChange,
		[]etcd.EnvironmentBlueprintServiceChange,
		[]etcd.EnvironmentBlueprintRouteChange,
		etcd.ReleaseGroupBlueprintPreparedMutation,
		etcd.ComponentTaskPreparation,
		etcd.BlueprintAttachTaskPreparation,
		etcd.BlueprintBackupPolicyPreparation,
		etcd.BlueprintScriptPublication,
		etcd.BlueprintReleasePublication,
		etcd.BlueprintRequirementGate,
		etcd.TaskRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	ListScripts(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ScriptRecord], error)
	PrepareBlueprintScriptPublication(
		context.Context,
		string,
		int64,
		string,
		[]etcd.Versioned[etcd.ScriptRecord],
		[]etcd.ScriptRecord,
		[]etcd.ScriptBodyGenerationRecord,
	) (etcd.BlueprintScriptPublication, error)
}
type environmentBlueprintService struct {
	volumeRoot        string
	environmentPool   netip.Prefix
	repository        environmentBlueprintRepository
	idempotency       *desiredrevision.Idempotency
	materials         environmentBlueprintMaterializationResolver
	releaseGroups     *controller.ReleaseGroupBlueprintPlanner
	blueprintReleases *blueprintrelease.Service
	entryGeneration   *EntryGenerationService
	attachFacts       *AttachFactService
	componentCatalog  []controller.EnvironmentComponentRegistration
	backups           environmentBlueprintBackupRepository
	backupKeys        environmentBlueprintBackupKeyFactory
	random            io.Reader
	now               func() time.Time
}
type environmentBlueprintMaterializationResolver interface {
	PinSecretValue(context.Context, string, string) (etcd.TaskSecretValueReference, error)
	ResolveTaskMaterializationSource(
		context.Context,
		string,
		etcd.TaskMaterializationSource,
	) ([]byte, error)
}
type durableEnvironmentBlueprintRepository struct {
	*etcd.EnvironmentBlueprintRepository
	desired    *desiredrevisionstore.Repository
	zones      *etcd.ZoneRepository
	services   *etcd.ServiceRepository
	routes     *etcd.RouteRepository
	entries    *etcd.EntryRepository
	values     *etcd.EntryValueGenerationRepository
	attaches   *etcd.AttachRepository
	components *etcd.ComponentRepository
	scripts    *etcd.ScriptRepository
	backups    *etcd.BackupPolicyRepository
	connectors *etcd.ConnectorRepository
}

func newDurableEnvironmentBlueprintRepository(
	hierarchy *etcd.EnvironmentBlueprintRepository,
	desired *desiredrevisionstore.Repository,
	zones *etcd.ZoneRepository,
	services *etcd.ServiceRepository,
	routes *etcd.RouteRepository,
	entries *etcd.EntryRepository,
	values *etcd.EntryValueGenerationRepository,
	attaches *etcd.AttachRepository,
	components *etcd.ComponentRepository,
	scripts *etcd.ScriptRepository,
) (*durableEnvironmentBlueprintRepository, error) {
	if hierarchy == nil || desired == nil || zones == nil || services == nil || routes == nil || entries == nil ||
		values == nil ||
		attaches == nil ||
		components == nil ||
		scripts == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint repositories are not configured")
	}
	return &durableEnvironmentBlueprintRepository{
		EnvironmentBlueprintRepository: hierarchy,
		desired:                        desired,
		zones:                          zones,
		services:                       services,
		routes:                         routes,
		entries:                        entries,
		values:                         values,
		attaches:                       attaches,
		components:                     components,
		scripts:                        scripts,
	}, nil
}
func (repository *durableEnvironmentBlueprintRepository) BlueprintEntryValueGenerationExists(
	ctx context.Context,
	record etcd.EntryRecord,
) (bool, error) {
	if record.Entry.Secret {
		value, found, err := repository.values.GetSecret(ctx, record.Entry.ID, record.CurrentValueGenerationID)
		defer clear(value.Ciphertext)
		return found && value.EnvironmentID == record.EnvironmentID, err
	}
	value, found, err := repository.values.GetPlain(ctx, record.Entry.ID, record.CurrentValueGenerationID)
	defer clear(value.Content)
	return found && value.EnvironmentID == record.EnvironmentID, err
}
func (repository *durableEnvironmentBlueprintRepository) CreateBlueprintEntryValueGeneration(
	ctx context.Context,
	generation etcd.EntryValueGeneration,
) error {
	if generation.Plain != nil && generation.Secret == nil {
		return repository.values.CreatePlain(ctx, *generation.Plain)
	}
	if generation.Secret != nil && generation.Plain == nil {
		return repository.values.CreateSecret(ctx, *generation.Secret)
	}
	return errs.New(errs.KindInternal, "Blueprint Entry value generation is invalid")
}
func (repository *durableEnvironmentBlueprintRepository) BindBlueprintEntryEnvironment(
	ctx context.Context,
	environmentID string,
	entryID string,
) error {
	return repository.entries.BindBlueprintEntryEnvironment(ctx, environmentID, entryID)
}
func (repository *durableEnvironmentBlueprintRepository) ResolveBlueprintEntryEnvironment(
	ctx context.Context,
	entryID string,
) (string, bool, error) {
	return repository.entries.ResolveBlueprintEntryEnvironment(ctx, entryID)
}
func (repository *durableEnvironmentBlueprintRepository) ClaimEnvironmentBlueprintStage(
	ctx context.Context,
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	return repository.desired.ClaimEnvironmentBlueprintStage(ctx, request)
}
func (repository *durableEnvironmentBlueprintRepository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	return repository.desired.StageEnvironmentBlueprintRevision(ctx, request)
}
func (repository *durableEnvironmentBlueprintRepository) AbandonEnvironmentBlueprintStage(
	ctx context.Context,
	claim etcd.EnvironmentBlueprintStageClaim,
) error {
	return repository.desired.AbandonEnvironmentBlueprintStage(ctx, claim)
}
func (repository *durableEnvironmentBlueprintRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.EntryRecord], error) {
	return repository.entries.ListEntries(ctx, environmentID, request)
}
func (repository *durableEnvironmentBlueprintRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.AttachRecord], error) {
	return repository.attaches.ListAttaches(ctx, environmentID, request)
}
func (repository *durableEnvironmentBlueprintRepository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ComponentRecord], error) {
	return repository.components.ListEnvironmentComponents(ctx, environmentID, request)
}
func (repository *durableEnvironmentBlueprintRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ZoneRecord], error) {
	return repository.zones.ListZones(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.RouteRecord], error) {
	return repository.routes.ListRoutes(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ScriptRecord], error) {
	return repository.scripts.ListScripts(ctx, environmentID, request)
}

func (repository *durableEnvironmentBlueprintRepository) PrepareBlueprintScriptPublication(
	ctx context.Context,
	environmentID string,
	readRevision int64,
	nextGenerationID string,
	current []etcd.Versioned[etcd.ScriptRecord],
	desired []etcd.ScriptRecord,
	generations []etcd.ScriptBodyGenerationRecord,
) (etcd.BlueprintScriptPublication, error) {
	return repository.scripts.PrepareBlueprintScriptPublication(
		ctx, environmentID, readRevision, nextGenerationID, current, desired, generations,
	)
}

func (repository *durableEnvironmentBlueprintRepository) PublishEnvironmentBlueprintDesiredRevision(
	ctx context.Context,
	environmentPool netip.Prefix,
	desiredNetworkPool string,
	project etcd.Versioned[etcd.ProjectRecord],
	environment etcd.Versioned[etcd.EnvironmentRecord],
	expectedHeadRevision int64,
	claim etcd.EnvironmentBlueprintStageClaim,
	revision etcd.EnvironmentDesiredRevisionIdentity,
	projection etcd.EnvironmentComposeProjection,
	zoneChanges []etcd.EnvironmentBlueprintZoneChange,
	serviceChanges []etcd.EnvironmentBlueprintServiceChange,
	routeChanges []etcd.EnvironmentBlueprintRouteChange,
	releaseGroupPreparation etcd.ReleaseGroupBlueprintPreparedMutation,
	componentPreparation etcd.ComponentTaskPreparation,
	attachPreparation etcd.BlueprintAttachTaskPreparation,
	backupPreparation etcd.BlueprintBackupPolicyPreparation,
	scriptPublication etcd.BlueprintScriptPublication,
	releasePublication etcd.BlueprintReleasePublication,
	requirementGate etcd.BlueprintRequirementGate,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.EnvironmentBlueprintRepository.PublishEnvironmentBlueprintDesiredRevision(
		ctx, environmentPool, desiredNetworkPool, project, environment, expectedHeadRevision,
		claim, revision, projection, zoneChanges, serviceChanges, routeChanges,
		releaseGroupPreparation, componentPreparation, attachPreparation,
		backupPreparation,
		scriptPublication, releasePublication, requirementGate, task, marker,
	)
}

func newEnvironmentBlueprintService(
	volumeRoot string,
	environmentPool string,
	repository environmentBlueprintRepository,
	idempotency *desiredrevision.Idempotency,
	materials environmentBlueprintMaterializationResolver,
	releaseGroups *controller.ReleaseGroupBlueprintPlanner,
	blueprintReleases *blueprintrelease.Service,
	entryGeneration *EntryGenerationService,
	attachFacts *AttachFactService,
	componentCatalog []controller.EnvironmentComponentRegistration,
) (*environmentBlueprintService, error) {
	parsedEnvironmentPool, poolErr := netip.ParsePrefix(environmentPool)
	if poolErr != nil || !parsedEnvironmentPool.Addr().Is4() ||
		parsedEnvironmentPool != parsedEnvironmentPool.Masked() ||
		repository == nil ||
		idempotency == nil ||
		materials == nil ||
		releaseGroups == nil ||
		entryGeneration == nil ||
		attachFacts == nil ||
		blueprintReleases == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	if _, err := controller.NewTaskPlanResolver(volumeRoot, componentCatalog); err != nil {
		return nil, err
	}
	return &environmentBlueprintService{
		volumeRoot: volumeRoot, environmentPool: parsedEnvironmentPool, repository: repository, idempotency: idempotency,
		materials: materials, releaseGroups: releaseGroups, blueprintReleases: blueprintReleases,
		entryGeneration:  entryGeneration,
		attachFacts:      attachFacts,
		componentCatalog: controller.CloneEnvironmentComponentCatalog(componentCatalog),
		random:           rand.Reader, now: time.Now,
	}, nil
}

func (service *environmentBlueprintService) ApplyBlueprint(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.applyBlueprint(ctx, environmentID, environmentID, bundle, expectedRevision, idempotencyKey, false)
}

func (service *environmentBlueprintService) ApplyComponentBlueprint(
	ctx context.Context,
	environmentID string,
	componentID string,
	bundle core.BlueprintBundle,
	expectedRevision, idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || expectedRevision == "" {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Component id or Blueprint revision is invalid",
		)
	}
	return service.applyBlueprint(ctx, environmentID, environmentID, bundle, expectedRevision, idempotencyKey, true)
}

func (service *environmentBlueprintService) applyBlueprint(
	ctx context.Context,
	environmentID string,
	taskTarget string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
	preserveRoutes bool,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Environment id is invalid")
	}
	if err := bundle.Validate(); err != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Blueprint bundle is invalid")
	}
	for attempt := 0; attempt < maximumEnvironmentBlueprintAttempts; attempt++ {
		response, err := service.applyBlueprintOnce(
			ctx,
			environmentID,
			taskTarget,
			bundle,
			expectedRevision,
			idempotencyKey,
			preserveRoutes,
		)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEnvironmentBlueprintAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint retry bound was not enforced")
}

func (service *environmentBlueprintService) applyBlueprintOnce(
	ctx context.Context,
	environmentID string,
	taskTarget string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
	preserveRoutes bool,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, desiredrevision.IntentAddress{
		Method: http.MethodPut, Route: environmentBlueprintRoute,
		Scope: idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: environmentID}},
	}, bundle)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.Durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
		Method: http.MethodPut, Route: environmentBlueprintRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Environment Blueprint replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}

	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	tenant, err := service.repository.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskOwner, err := etcd.EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if environment.Record.ProjectID != project.Record.ID || project.Record.TenantID != tenant.Record.ID ||
		project.Record.Kind != etcd.ProjectKindTenant {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment hierarchy is inconsistent")
	}

	head, hasHead, err := service.repository.GetEnvironmentBlueprintHead(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	previousProjection, hasProjection, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	expectedHeadRevision, previous, generation, err := environmentBlueprintState(
		environmentID, head, hasHead, previousProjection, hasProjection,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if expectedRevision != "" && expectedRevision != environmentBlueprintRevision(head, hasHead) {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint changed after the authoring revision was loaded",
		)
	}
	if generation > math.MaxInt32 {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment render generation is exhausted")
	}
	candidateTaskID := ids.New(ids.KindTask)
	candidateCreatedAt := service.now().UTC()

	if preserveRoutes && hasProjection {
		// Component-only edits preserve the selected native workload, including
		// its companion files. The revision guard above binds these bytes to
		// the authoring document; external Blueprint inputs remain closed.
		bundle.Files = append(append([]core.BlueprintFile(nil), bundle.Files...), previousProjection.Record.RuntimeFiles...)
		sort.Slice(bundle.Files, func(left, right int) bool { return bundle.Files[left].Path < bundle.Files[right].Path })
	}
	parsed, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID,
		Tenant:        tenant.Record.Slug, Project: project.Record.Slug, Environment: environment.Record.Name,
	}, bundle)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if err := controller.ValidateEnvironmentBlueprintAvailability(parsed); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentAttaches, attachReadRevision, err := service.listBlueprintAttaches(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	requirements, err := environmentBlueprintRequirements(
		parsed.Extensions.Requires,
		parsed.Extensions.Attachments,
		currentAttaches,
		attachReadRevision,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	submittedServiceNames := environmentBlueprintServiceNames(parsed.Project)
	var priorProject *composetypes.Project
	if hasProjection {
		priorProject, err = controller.LoadNormalizedEnvironmentProject(ctx, previousProjection.Record)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	desiredEnvironment := environment
	desiredEnvironment.Record.NetworkPool = parsed.Extensions.NetworkPool
	if err := preserveEnvironmentBlueprintResources(
		parsed.Project, priorProject, previous, previousProjection.Record.Volumes, hasProjection,
	); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	preflightServices, err := service.listBlueprintServices(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	preflightExtensions, err := preserveEnvironmentBlueprintServiceExtensions(
		parsed.ServiceExtensions, submittedServiceNames, previous.Services, previousProjection.Record.ServiceExtensions,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	workloads, err := service.blueprintReleases.PreflightBlueprint(ctx, blueprintrelease.BlueprintPreflightInput{
		EnvironmentID: environmentID, Project: parsed.Project, PriorProject: priorProject,
		PreviousIdentities: previous, ServiceExtensions: preflightExtensions,
		CurrentServices: preflightServices, AuthoredGroups: parsed.Extensions.ReleaseGroups,
	}, service.releaseGroups)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	claim, err := desiredrevision.Claim(
		ctx, service.repository, desiredrevision.ClaimInput{
			EnvironmentID: environmentID, CandidateTaskID: candidateTaskID,
			Locator: locator, Intent: evidence.Durable, BaselineHeadRevision: expectedHeadRevision,
			SourceKind: etcd.EnvironmentBlueprintSourceApply,
			MatchExistingIntent: func(ctx context.Context, existing etcd.ProtectedIntentRecord) (bool, error) {
				return service.idempotency.MatchesStaged(ctx, evidence, existing)
			},
			RenderGeneration: generation, CreatedAt: candidateCreatedAt,
		})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	taskID := claim.TaskID
	now := claim.CreatedAt
	releaseMemberships, err := blueprintrelease.BuildNormalizedServiceMemberships(priorProject, parsed.Project)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	normalizedCompose, err := controller.MarshalNormalizedEnvironmentProject(parsed.Project)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	runtimeFiles, err := blueprintparser.SelectRuntimeFiles(
		parsed.Project,
		bundle.Files,
		previousProjection.Record.RuntimeFiles,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	volumeSlugs, err := environmentBlueprintVolumeSlugs(
		parsed.Project,
		previousProjection.Record,
		hasProjection,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	changes, err := controller.ReconcileOwnedComposeIdentities(parsed.Project, previous, allocator.New)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if len(changes.RemovedServiceIDs) != 0 || len(changes.RemovedNetworkIDs) != 0 ||
		len(changes.RemovedVolumeIDs) != 0 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing owned resource; remove it explicitly before apply",
		)
	}
	zoneOwnerKind := core.ZoneOwnerEnvironment
	zoneOwnerID := environmentID
	if project.Record.Kind == etcd.ProjectKindBacking {
		zoneOwnerKind = core.ZoneOwnerBackingProject
		zoneOwnerID = project.Record.ID
	}
	desiredZones, err := controller.ProjectZoneProjection(
		parsed.Project,
		changes.Current,
		zoneOwnerKind,
		zoneOwnerID,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentZones, err := service.listBlueprintZones(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	zoneChanges, err := prepareEnvironmentBlueprintZoneChanges(environmentID, desiredZones, currentZones)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentServices, err := service.listBlueprintServices(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	serviceExtensions, err := preserveEnvironmentBlueprintServiceExtensions(
		parsed.ServiceExtensions,
		submittedServiceNames,
		previous.Services,
		previousProjection.Record.ServiceExtensions,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	desiredServices, err := controller.ProjectServiceProjection(
		parsed.Project,
		changes.Current,
		serviceExtensions,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	serviceChanges, err := blueprintrelease.PrepareServiceChanges(
		environmentID, desiredServices, currentServices,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	scriptRepository := service.repository
	currentScripts, scriptsReadRevision, err := service.listBlueprintScripts(ctx, environmentID, scriptRepository)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	scriptServices := make([]etcd.ServiceRecord, len(desiredServices))
	for index, desiredService := range desiredServices {
		scriptServices[index] = etcd.ServiceRecord{EnvironmentID: environmentID, Desired: desiredService}
	}
	previousScripts := make([]etcd.ScriptRecord, len(currentScripts))
	for index, currentScript := range currentScripts {
		previousScripts[index] = currentScript.Record
	}
	reconciledScripts, err := desiredrevision.ReconcileBlueprintScripts(
		environmentID,
		parsed.Extensions.Scripts,
		scriptServices,
		previousScripts,
		allocator.Named,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	scriptPublication, err := scriptRepository.PrepareBlueprintScriptPublication(
		ctx,
		environmentID,
		scriptsReadRevision,
		claim.RevisionID,
		currentScripts,
		reconciledScripts.Current,
		reconciledScripts.BodyGenerations,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer scriptPublication.Clear()
	serviceNames := make(map[string]string, len(desiredServices))
	for _, desired := range desiredServices {
		serviceNames[desired.ID] = desired.Name
	}
	effectiveReleaseGroups, err := service.releaseGroups.AuthoringSpecs(ctx, environmentID, serviceNames)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for name, spec := range parsed.Extensions.ReleaseGroups {
		effectiveReleaseGroups[name] = spec
	}
	releaseGroupPreparation, err := service.releaseGroups.Prepare(
		ctx,
		environmentID,
		effectiveReleaseGroups,
		desiredServices,
		func() string { return allocator.New(ids.KindReleaseGroup) },
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentRoutes, err := service.listBlueprintRoutes(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !preserveRoutes {
		parsed.Extensions.Routes, err = preserveEnvironmentBlueprintRoutes(
			parsed.Extensions.Routes, desiredServices, currentRoutes,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	previousRoutes := make([]controller.RouteIdentity, len(currentRoutes))
	for index, route := range currentRoutes {
		previousRoutes[index] = controller.RouteIdentity{
			ID: route.Record.Desired.ID, Host: route.Record.Desired.Host, Path: route.Record.Desired.Path,
		}
	}
	reconciledRoutes := controller.BlueprintRouteChanges{}
	if preserveRoutes {
		reconciledRoutes.Current = make([]core.Route, len(currentRoutes))
		for index, route := range currentRoutes {
			reconciledRoutes.Current[index] = route.Record.Desired
		}
	} else {
		reconciledRoutes, err = controller.ReconcileBlueprintRoutes(
			parsed.Extensions.Routes,
			desiredServices,
			previousRoutes,
			allocator.New,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
	}
	if len(reconciledRoutes.RemovedRouteIDs) != 0 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	routeChanges, err := prepareEnvironmentBlueprintRouteChanges(
		environmentID, reconciledRoutes.Current, currentRoutes,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentComponents, err := service.listBlueprintComponents(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	currentEntries, err := service.listBlueprintEntries(ctx, environmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	componentPreparation, pinnedComponents, effectiveComponents, err := service.prepareBlueprintComponents(
		ctx,
		environmentID,
		taskID,
		now,
		allocator.New,
		parsed.Extensions.Components,
		currentComponents,
		zoneChanges,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	preparedAttaches, err := service.prepareBlueprintAttaches(
		ctx, environmentID, taskID, parsed.Extensions.Attachments, serviceChanges, currentAttaches,
		allocator.Named, componentTaskPreparationIsZeroForBlueprint(componentPreparation), now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer preparedAttaches.clear()
	pinnedEntries, err := environmentEntryProjection(currentEntries)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	reconciledEntries, err := controller.ReconcileBlueprintEntries(
		environmentID, parsed.Extensions.Entries, pinnedEntries, allocator.Named,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	componentEnvironment := blueprintComponentEnvironment(
		desiredEnvironment.Record,
		desiredZones,
		desiredServices,
		reconciledRoutes.Current,
		effectiveComponents,
		reconciledEntries.Current,
	)
	componentProjection, err := controller.ProjectEnvironmentComponents(
		parsed.Project,
		componentEnvironment,
		service.componentCatalog,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	renderIdentities := environmentComponentComposeIdentities(changes.Current, componentProjection.Services)
	entryProjection, err := controller.ProjectEnvironmentEntries(
		componentProjection.Project,
		environmentID,
		environment.Record.VolumeDir,
		changes.Current.Services,
		reconciledEntries.Current,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	entryRemovals, err := environmentBlueprintEntryRemovals(
		environmentID, reconciledEntries.Removed, reconciledEntries.Current, renderIdentities.Services,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	entryProjection.Materializations = append(entryProjection.Materializations, entryRemovals...)
	componentProjection.Project = entryProjection.Project
	attachZones, attachServices, _ := environmentBlueprintTopologyProjection(zoneChanges, serviceChanges, nil)
	attachProjection := etcd.EnvironmentComposeProjection{
		EnvironmentID:   environmentID,
		DesiredZones:    attachZones,
		DesiredServices: attachServices,
	}
	attachJoins, err := resolveAttachNetworkJoins(environmentID, attachProjection, preparedAttaches.effective, "")
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	externalNetworks, err := controller.ProjectAttachNetworks(
		componentProjection.Project,
		attachProjection,
		attachJoins,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	volumeMounts, err := environmentBlueprintVolumeMounts(componentProjection.Project, renderIdentities)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	planID := allocator.Named(ids.KindPlan, "execution-plan")
	artifactID := allocator.Named(ids.KindConfig, "compose-artifact")
	artifact, err := controller.RenderCompose(controller.ComposeRenderInput{
		Project: componentProjection.Project, ArtifactID: artifactID,
		ProjectOwnerKind: controller.ComposeProjectOwnerTenant,
		TenantID:         tenant.Record.ID, ProjectID: project.Record.ID, EnvironmentID: environmentID,
		PlanID: planID, RenderGeneration: generation, AuthorizedVolumeDir: environment.Record.VolumeDir,
		Identities: renderIdentities, ExternalNetworks: externalNetworks,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	artifact, err = service.blueprintReleases.PrepareRuntimeArtifact(workloads, artifact, serviceChanges)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	artifactValue, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	entryGeneration := *service.entryGeneration
	entryGeneration.facts = preparedAttaches.facts
	if err := service.prepareBlueprintEntryValues(
		ctx, &entryGeneration, project.Record.ID, environmentID, reconciledEntries, now,
	); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	materializations, materializationSteps, err := service.environmentComponentMaterializations(
		ctx,
		environmentID,
		project.Record.ID,
		taskID,
		artifactID,
		allocator.Named,
		runtimeFiles,
		componentProjection,
		entryProjection.Materializations,
		reconciledEntries.Current,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	componentSteps, _, err := controller.BuildEnvironmentComponentTaskContribution(
		controller.EnvironmentComponentTaskContributionInput{
			Apply: controller.EnvironmentManagedConfigApplyInput{
				RevisionID: taskID, RenderGeneration: generation, Components: pinnedComponents,
				ComponentCatalog: service.componentCatalog,
				Materializations: materializations, Artifact: artifact,
			},
			AllocateStep: func() string {
				return allocator.Named(ids.KindStep, "http-router-config-activate")
			},
			TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds),
		},
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	steps := append([]*agentpb.ExecutionStep(nil), materializationSteps...)
	stepRecords := make([]etcd.TaskStepRecord, 0, len(materializationSteps)+4)
	for _, step := range materializationSteps {
		stepRecords = append(stepRecords, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: step.StepId})
	}
	managedVolumeIDs := managedEnvironmentVolumeIDs(changes.Current.Volumes)
	var volumeIntentDigest []byte
	if len(managedVolumeIDs) != 0 {
		intentDigest, decodeErr := hex.DecodeString(evidence.Durable.CiphertextDigest)
		if decodeErr != nil || len(intentDigest) != sha256.Size {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Blueprint protected intent digest is invalid",
			)
		}
		volumeIntentDigest = intentDigest
		stepID := allocator.Named(ids.KindStep, "managed-volume-directories")
		steps = append(steps, &agentpb.ExecutionStep{
			StepId: stepID, TimeoutSeconds: uint32(environmentBlueprintTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId: artifactID, VolumeIds: managedVolumeIDs,
					IntentSha256: append([]byte(nil), intentDigest...),
				},
			},
		})
		stepRecords = append(stepRecords, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: stepID})
	}
	attachSteps, attachStepRecords, err := preparedAttaches.procedureSteps(
		taskID, environmentBlueprintTimeoutSeconds, allocator.Named,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clearBlueprintAttachProcedureSteps(attachSteps)
	steps = append(steps, attachSteps...)
	stepRecords = append(stepRecords, attachStepRecords...)
	dependencyPlans, err := buildEnvironmentDependencyPlans(renderIdentities.Services, serviceExtensions)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	revision := desiredrevision.BlueprintRevision(environmentID, taskID, now, bundle)
	projection := desiredrevision.ComposeProjection(
		environmentID,
		taskID,
		generation,
		renderIdentities,
		volumeSlugs,
		volumeMounts,
		artifactValue,
		normalizedCompose,
		runtimeFiles,
		serviceExtensions,
		reconciledRoutes.Current,
		pinnedComponents,
		reconciledEntries.Current,
	)
	projection.ManagedComponentRuntimeSources = append(
		[]etcd.ManagedComponentRuntimeSource(nil),
		previousProjection.Record.ManagedComponentRuntimeSources...,
	)
	projection.ManagedComponentRuntimeSources, err = etcd.ProjectManagedComponentRuntimeSources(
		componentPreparation,
		projection,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	topologyZones, topologyServices, topologyRoutes := environmentBlueprintTopologyProjection(
		zoneChanges, serviceChanges, routeChanges,
	)
	projection = desiredrevision.WithDesiredTopology(projection, topologyZones, topologyServices, topologyRoutes)
	projection.ServiceDependencyPlans = dependencyPlans.Clone()
	projection.BlueprintRequirements = requirements.Clone()
	backup, backupPreparation, err := service.prepareEnvironmentBlueprintBackup(
		ctx, environmentID, taskID, attachReadRevision, parsed.Extensions.Backup,
		projection, preparedAttaches, allocator.Named, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer backupPreparation.Clear()
	projection.Backup = backup
	stagedPublication, err := desiredrevision.Stage(ctx, service.repository, desiredrevision.StageInput{
		Claim: claim, Blueprint: revision, Projection: projection,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	params := map[string]string{
		etcd.EnvironmentDesiredRevisionParam:         taskID,
		etcd.TaskMaterializationEnvironmentParam:     environmentID,
		controller.EnvironmentBlueprintArtifactParam: artifactID,
		taskcontract.EnvironmentBlueprintProcedureParam: string(
			taskcontract.BlueprintComposeProcedureNone,
		),
	}
	if len(managedVolumeIDs) != 0 {
		params[controller.EnvironmentBlueprintManagedVolumesParam] = strings.Join(managedVolumeIDs, ",")
		params[controller.VolumeTaskIntentSHA256Param] = hex.EncodeToString(volumeIntentDigest)
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: allocator.Named(ids.KindOperation, "operation"), IdempotencyKey: idempotencyKey,
		Owner: taskOwner, Actor: etcd.TaskActorOperator,
		Executor: etcd.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: int32(generation), Type: etcd.TaskUpdate, Target: taskTarget,
		Params: params, Steps: stepRecords, TimeoutSeconds: environmentBlueprintTimeoutSeconds,
		Materializations: materializations,
		Status:           etcd.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	task.ManagedComponentTeardownSources = componentPreparation.ManagedComponentTeardownSources()
	preparedRelease, err := service.blueprintReleases.Prepare(ctx, blueprintrelease.PrepareInput{
		IntendedAttaches: preparedAttaches.effective,
		Workloads:        workloads,
		VolumeRoot:       service.volumeRoot,
		Tenant:           tenant, Project: project, Environment: environment,
		Projection: projection, ServiceChanges: serviceChanges, Memberships: releaseMemberships,
		Scripts:       reconciledScripts.Current,
		ReleaseGroups: effectiveReleaseGroups, Task: task,
		PrefixSteps: steps, ComponentSteps: componentSteps, Artifact: artifact,
		AllocateNamed: allocator.Named, CreatedAt: now,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, desiredrevision.Abandon(ctx, service.repository, stagedPublication, err)
	}
	task = preparedRelease.Task
	abandonPrepared := func(cause error) error {
		return desiredrevision.AbandonBlueprint(
			ctx, service.repository, stagedPublication, preparedRelease.Publication, cause,
		)
	}
	requirementGate := etcd.BlueprintRequirementGate{}
	if len(requirements.Resolved) != 0 {
		inherited, gateErr := environmentBlueprintRequirementTaskEdges(
			ctx, service.repository, task.ID, requirements, requirements.ResolutionRevision,
		)
		if gateErr != nil {
			return etcd.IdempotencyResponse{}, abandonPrepared(gateErr)
		}
		stepIDs := make([]string, len(task.Steps))
		for index, step := range task.Steps {
			stepIDs[index] = step.ID
		}
		dag, gateErr := core.BuildBlueprintRequirementDAG(task.ID, requirements, stepIDs, inherited)
		if gateErr != nil {
			return etcd.IdempotencyResponse{}, abandonPrepared(gateErr)
		}
		requirementGate, gateErr = etcd.NewBlueprintRequirementGate(
			task, requirements.ResolutionRevision, dag,
		)
		if gateErr != nil {
			return etcd.IdempotencyResponse{}, abandonPrepared(gateErr)
		}
		task.Params[etcd.TaskBlueprintRequirementGateSHA256Param] = requirementGate.DAGDigest
	}
	routeProvider, routeProjection, err := controller.ResolveComponentTaskRouteProvider(
		service.componentCatalog,
		componentEnvironment,
		pinnedComponents,
		componentPreparation.Intent.Candidates,
		int64(generation),
		generation,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, abandonPrepared(err)
	}
	if routeProjection {
		if routeProvider != nil {
			provider := etcd.RouteProviderObservation{
				ComponentID:      routeProvider.ComponentID,
				DefinitionDigest: routeProvider.DefinitionDigest,
				CatalogDigest:    routeProvider.CatalogDigest,
				InputRevision:    routeProvider.InputRevision,
				InputGeneration:  routeProvider.InputGeneration,
			}
			for index, change := range routeChanges {
				change.Record, err = etcd.SetRouteObservation(change.Record, etcd.RouteObservation{
					Status:            etcd.RouteObservedPending,
					DesiredGeneration: change.Record.DesiredGeneration,
					Provider:          provider,
				})
				if err != nil {
					return etcd.IdempotencyResponse{}, abandonPrepared(err)
				}
				routeChanges[index] = change
			}
		}
		routeRecords := make([]etcd.RouteRecord, len(routeChanges))
		for index, change := range routeChanges {
			routeRecords[index] = change.Record
		}
		componentPreparation, err = etcd.WithComponentTaskRouteProjection(
			componentPreparation, routeRecords, routeProvider,
		)
		if err != nil {
			return etcd.IdempotencyResponse{}, abandonPrepared(err)
		}
	}
	return desiredrevision.Publish(ctx, service.repository, service.idempotency, desiredrevision.PublishInput{
		Project: project, Environment: environment, EnvironmentPool: service.environmentPool,
		NetworkPool: desiredEnvironment.Record.NetworkPool, ExpectedHeadRevision: expectedHeadRevision,
		Staged: stagedPublication, Evidence: evidence, Locator: locator,
		ZoneChanges: zoneChanges, ServiceChanges: serviceChanges, RouteChanges: routeChanges,
		ReleaseGroupPreparation: releaseGroupPreparation,
		ComponentPreparation:    componentPreparation,
		AttachPreparation:       preparedAttaches.publication,
		BackupPreparation:       backupPreparation,
		ScriptPublication:       scriptPublication,
		ReleasePublication:      preparedRelease.Publication,
		RequirementGate:         requirementGate,
		Task:                    task,
	})
}

func componentTaskPreparationIsZeroForBlueprint(preparation etcd.ComponentTaskPreparation) bool {
	return preparation.Intent.TaskID == ""
}

func (service *environmentBlueprintService) listBlueprintZones(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ZoneRecord], error) {
	zones := []etcd.Versioned[etcd.ZoneRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListZones(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		zones = append(zones, page.Items...)
		if page.NextCursor == "" {
			return zones, nil
		}
		cursor = page.NextCursor
	}
}

func prepareEnvironmentBlueprintZoneChanges(
	environmentID string,
	desired []core.Zone,
	current []etcd.Versioned[etcd.ZoneRecord],
) ([]etcd.EnvironmentBlueprintZoneChange, error) {
	currentByID := make(map[string]etcd.Versioned[etcd.ZoneRecord], len(current))
	for _, zone := range current {
		if zone.Record.EnvironmentID != environmentID || zone.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state is inconsistent")
		}
		if _, duplicate := currentByID[zone.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Zone state repeats an id")
		}
		currentByID[zone.Record.Desired.ID] = zone
	}
	changes := make([]etcd.EnvironmentBlueprintZoneChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			if existing.Record.Desired != next {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint changed an immutable Zone; add a new Zone and move Services explicitly",
				)
			}
			currentCopy := existing
			changes = append(changes, etcd.EnvironmentBlueprintZoneChange{
				Current: &currentCopy,
				Record:  existing.Record,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := etcd.NewZoneRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, etcd.EnvironmentBlueprintZoneChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Zone; remove it explicitly after moving Services",
		)
	}
	return changes, nil
}

func (service *environmentBlueprintService) listBlueprintServices(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ServiceRecord], error) {
	services := []etcd.Versioned[etcd.ServiceRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListServices(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		services = append(services, page.Items...)
		if page.NextCursor == "" {
			return services, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintScripts(
	ctx context.Context,
	environmentID string,
	repository environmentBlueprintRepository,
) ([]etcd.Versioned[etcd.ScriptRecord], int64, error) {
	scripts := []etcd.Versioned[etcd.ScriptRecord](nil)
	cursor := ""
	readRevision := int64(0)
	for {
		page, err := repository.ListScripts(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, 0, err
		}
		if readRevision == 0 {
			readRevision = page.Revision
		} else if page.Revision != readRevision {
			return nil, 0, errs.New(errs.KindStateConflict, "Blueprint Script snapshot changed while listing")
		}
		scripts = append(scripts, page.Items...)
		if page.NextCursor == "" {
			return scripts, readRevision, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintRoutes(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.RouteRecord], error) {
	routes := []etcd.Versioned[etcd.RouteRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListRoutes(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		routes = append(routes, page.Items...)
		if page.NextCursor == "" {
			return routes, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintComponents(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.ComponentRecord], error) {
	componentRecords := []etcd.Versioned[etcd.ComponentRecord](nil)
	cursor := ""
	for {
		page, err := service.repository.ListEnvironmentComponents(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		componentRecords = append(componentRecords, page.Items...)
		if page.NextCursor == "" {
			return componentRecords, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintEntries(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.EntryRecord], error) {
	entriesByID := make(map[string]etcd.Versioned[etcd.EntryRecord])
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil {
		return nil, err
	}
	if found {
		for _, record := range projection.Record.Entries {
			entriesByID[record.Entry.ID] = etcd.Versioned[etcd.EntryRecord]{
				Record: record, Revision: projection.Revision, ReadRevision: projection.ReadRevision,
			}
		}
	}
	cursor := ""
	for {
		page, err := service.repository.ListEntries(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if _, projected := entriesByID[item.Record.Entry.ID]; !projected {
				entriesByID[item.Record.Entry.ID] = item
			}
		}
		if page.NextCursor == "" {
			entries := make([]etcd.Versioned[etcd.EntryRecord], 0, len(entriesByID))
			for _, item := range entriesByID {
				entries = append(entries, item)
			}
			sort.Slice(entries, func(left, right int) bool {
				return entries[left].Record.Entry.ID < entries[right].Record.Entry.ID
			})
			return entries, nil
		}
		cursor = page.NextCursor
	}
}

func (service *environmentBlueprintService) listBlueprintAttaches(
	ctx context.Context,
	environmentID string,
) ([]etcd.Versioned[etcd.AttachRecord], int64, error) {
	attaches := []etcd.Versioned[etcd.AttachRecord](nil)
	cursor := ""
	revision := int64(0)
	for {
		page, err := service.repository.ListAttaches(
			ctx,
			environmentID,
			etcd.PageRequest{Limit: etcd.MaximumPageLimit, Cursor: cursor},
		)
		if err != nil {
			return nil, 0, err
		}
		if page.Revision <= 0 || revision != 0 && page.Revision != revision {
			return nil, 0, errs.New(errs.KindInternal, "Blueprint Attach topology pages changed revision")
		}
		revision = page.Revision
		attaches = append(attaches, page.Items...)
		if page.NextCursor == "" {
			return attaches, revision, nil
		}
		cursor = page.NextCursor
	}
}

func environmentBlueprintTopologyProjection(
	zones []etcd.EnvironmentBlueprintZoneChange,
	services []etcd.EnvironmentBlueprintServiceChange,
	routes []etcd.EnvironmentBlueprintRouteChange,
) ([]etcd.EnvironmentZoneProjection, []etcd.EnvironmentServiceProjection, []etcd.EnvironmentRouteProjection) {
	zoneProjection := make([]etcd.EnvironmentZoneProjection, len(zones))
	for index, change := range zones {
		zoneProjection[index] = etcd.EnvironmentZoneProjection{
			EnvironmentID: change.Record.EnvironmentID, Desired: change.Record.Desired,
		}
	}
	serviceProjection := make([]etcd.EnvironmentServiceProjection, len(services))
	for index, change := range services {
		serviceProjection[index] = etcd.EnvironmentServiceProjection{
			EnvironmentID:    change.Record.EnvironmentID,
			BackingNetworkID: change.Record.BackingNetworkID,
			Desired:          change.Record.Desired,
		}
	}
	routeProjection := make([]etcd.EnvironmentRouteProjection, len(routes))
	for index, change := range routes {
		routeProjection[index] = etcd.EnvironmentRouteProjection{
			EnvironmentID:     change.Record.EnvironmentID,
			Desired:           change.Record.Desired,
			DesiredGeneration: change.Record.DesiredGeneration,
		}
	}
	return zoneProjection, serviceProjection, routeProjection
}

func (service *environmentBlueprintService) prepareBlueprintComponents(
	ctx context.Context,
	environmentID string,
	taskID string,
	createdAt time.Time,
	allocate func(ids.Kind) string,
	specs map[string]core.ComponentSpec,
	current []etcd.Versioned[etcd.ComponentRecord],
	zoneChanges []etcd.EnvironmentBlueprintZoneChange,
) (etcd.ComponentTaskPreparation, []etcd.ComponentRecord, []core.Component, error) {
	currentComponents := make([]core.Component, len(current))
	currentByID := make(map[string]etcd.Versioned[etcd.ComponentRecord], len(current))
	for index, versioned := range current {
		component, err := etcd.ProjectComponentRecord(versioned.Record)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
		if _, duplicate := currentByID[component.ID]; duplicate {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Environment Component singleton set repeats an id",
			)
		}
		currentComponents[index] = component
		currentByID[component.ID] = versioned
	}
	changes, err := controller.ReconcileBlueprintComponents(specs, currentComponents, allocate)
	if err != nil {
		return etcd.ComponentTaskPreparation{}, nil, nil, err
	}
	inputs := make([]etcd.EnvironmentComponentCandidateInput, len(changes.Candidates))
	for index, candidate := range changes.Candidates {
		versioned, exists := currentByID[candidate.Current.ID]
		if !exists {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"Blueprint Component candidate lost its active record",
			)
		}
		inputs[index] = etcd.EnvironmentComponentCandidateInput{
			Current: versioned, Candidate: candidate.Candidate,
		}
	}
	preparation := etcd.ComponentTaskPreparation{}
	if len(inputs) != 0 {
		preparation, err = service.repository.PrepareEnvironmentComponentTask(
			ctx,
			taskID,
			environmentID,
			zoneChanges,
			inputs,
			createdAt,
		)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
	}
	recordsByID := make(map[string]etcd.ComponentRecord, len(changes.Effective))
	for _, component := range changes.Effective {
		record, recordErr := etcd.NewComponentRecord(component)
		if recordErr != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, recordErr
		}
		recordsByID[component.ID] = record
	}
	if len(preparation.Intent.Candidates) != len(changes.Candidates) {
		return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
			errs.KindInternal,
			"prepared Blueprint Component candidate count changed",
		)
	}
	for _, candidate := range preparation.Intent.Candidates {
		if _, exists := recordsByID[candidate.Candidate.Desired.ID]; !exists {
			return etcd.ComponentTaskPreparation{}, nil, nil, errs.New(
				errs.KindInternal,
				"prepared Blueprint Component candidate is unknown",
			)
		}
		recordsByID[candidate.Candidate.Desired.ID] = candidate.Candidate
	}
	records := make([]etcd.ComponentRecord, 0, len(recordsByID))
	for _, record := range recordsByID {
		records = append(records, record)
	}
	sort.Slice(records, func(left int, right int) bool {
		return records[left].Desired.Kind < records[right].Desired.Kind
	})
	effective := make([]core.Component, len(records))
	for index, record := range records {
		effective[index], err = etcd.ProjectComponentRecord(record)
		if err != nil {
			return etcd.ComponentTaskPreparation{}, nil, nil, err
		}
	}
	return preparation, records, effective, nil
}

func environmentEntryProjection(
	current []etcd.Versioned[etcd.EntryRecord],
) ([]etcd.EntryRecord, error) {
	records := make([]etcd.EntryRecord, len(current))
	for index, versioned := range current {
		var err error
		records[index], err = etcd.NewEntryRecord(
			versioned.Record.EnvironmentID,
			versioned.Record.Entry,
			versioned.Record.CurrentValueGenerationID,
		)
		if err != nil {
			return nil, err
		}
		records[index].BlueprintKey = versioned.Record.BlueprintKey
	}
	sort.Slice(records, func(left int, right int) bool {
		return records[left].Entry.ID < records[right].Entry.ID
	})
	return records, nil
}

func blueprintComponentEnvironment(
	environment etcd.EnvironmentRecord,
	zones []core.Zone,
	services []core.Service,
	routes []core.Route,
	components []core.Component,
	entries []etcd.EntryRecord,
) core.Environment {
	projected := core.Environment{
		ID: environment.ID, ProjectID: environment.ProjectID, Name: environment.Name,
		NetworkPool: environment.NetworkPool, VolumeDir: environment.VolumeDir,
		Zones: make(map[string]core.Zone, len(zones)), Services: make(map[string]core.Service, len(services)),
		Routes: append([]core.Route(nil), routes...), Components: append([]core.Component(nil), components...),
		Entries: projectedEnvironmentEntries(entries),
	}
	for _, zone := range zones {
		projected.Zones[zone.Name] = zone
	}
	for _, service := range services {
		projected.Services[service.Name] = service
	}
	return projected
}

func projectedEnvironmentEntries(records []etcd.EntryRecord) []core.EnvEntry {
	entries := make([]core.EnvEntry, len(records))
	for index, record := range records {
		entries[index] = record.Entry
	}
	return entries
}

func environmentComponentComposeIdentities(
	authored controller.ComposeIdentitySnapshot,
	generated []controller.ComposeResourceIdentity,
) controller.ComposeIdentitySnapshot {
	result := controller.ComposeIdentitySnapshot{
		Services: append([]controller.ComposeResourceIdentity(nil), authored.Services...),
		Networks: append([]controller.ComposeResourceIdentity(nil), authored.Networks...),
		Volumes:  append([]controller.ComposeResourceIdentity(nil), authored.Volumes...),
	}
	result.Services = append(result.Services, generated...)
	sort.Slice(result.Services, func(left int, right int) bool {
		return result.Services[left].Name < result.Services[right].Name
	})
	return result
}

type environmentComponentMaterializationInput struct {
	destination string
	serviceID   string
	serviceName string
	outputKind  etcd.TaskMaterializationOutputKind
	uid         uint32
	gid         uint32
	mode        entrymaterialization.Mode
	source      etcd.TaskMaterializationSource
	content     []byte
	resolve     bool
}

func (service *environmentBlueprintService) environmentComponentMaterializations(
	ctx context.Context,
	environmentID string,
	projectID string,
	revisionID string,
	artifactID string,
	allocate func(ids.Kind, string) string,
	runtimeFiles []core.BlueprintFile,
	projection controller.EnvironmentComponentComposeProjection,
	entryMaterializations []controller.EnvironmentEntryMaterialization,
	entries []etcd.EntryRecord,
) ([]etcd.TaskMaterializationRecord, []*agentpb.ExecutionStep, error) {
	inputs := make([]environmentComponentMaterializationInput, 0,
		len(runtimeFiles)+len(projection.PlainFiles)+len(projection.EnvironmentFiles)+len(entryMaterializations))
	for _, materialization := range entryMaterializations {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: materialization.Destination,
			serviceID:   materialization.ServiceID,
			serviceName: materialization.ServiceName,
			outputKind:  materialization.OutputKind,
			uid:         materialization.UID,
			gid:         materialization.GID,
			mode:        materialization.Mode,
			source:      materialization.Source,
			resolve:     true,
		})
	}
	for _, file := range runtimeFiles {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Path, outputKind: etcd.TaskMaterializationOutputPlainFile,
			mode: entrymaterialization.ModeReadOnly,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceBlueprintFile,
				BlueprintFile: &etcd.TaskBlueprintFileValueReference{
					RevisionID: revisionID, Path: file.Path,
				},
			},
			content: append([]byte(nil), file.Content...),
		})
	}
	for _, file := range projection.PlainFiles {
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Path, outputKind: etcd.TaskMaterializationOutputPlainFile,
			mode: entrymaterialization.ModeReadOnly,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceComponentFile,
				ComponentFile: &etcd.TaskComponentFileValueReference{
					RevisionID: revisionID, ComponentID: file.ComponentID, Path: file.Path,
				},
			},
			content: append([]byte(nil), file.Content...),
		})
	}
	for _, file := range projection.EnvironmentFiles {
		values := make([]etcd.TaskGeneratedEnvironmentEntryReference, len(file.Values))
		for index, binding := range file.Values {
			secret, err := service.materials.PinSecretValue(ctx, projectID, binding.SecretID)
			if err != nil {
				return nil, nil, err
			}
			values[index] = etcd.TaskGeneratedEnvironmentEntryReference{
				Name: binding.Name, Secret: &secret,
			}
		}
		sort.Slice(values, func(left int, right int) bool { return values[left].Name < values[right].Name })
		inputs = append(inputs, environmentComponentMaterializationInput{
			destination: file.Destination, serviceID: file.ServiceID, serviceName: file.ServiceName,
			outputKind: etcd.TaskMaterializationOutputGeneratedEnvironment,
			mode:       entrymaterialization.ModePrivate, resolve: true,
			source: etcd.TaskMaterializationSource{
				Kind: etcd.TaskMaterializationSourceGeneratedEnvironment,
				GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{
					FormatVersion: 1, Values: values,
				},
			},
		})
	}
	sort.Slice(inputs, func(left int, right int) bool { return inputs[left].destination < inputs[right].destination })
	references := make([]etcd.TaskMaterializationRecord, 0, len(inputs))
	steps := make([]*agentpb.ExecutionStep, 0, len(inputs))
	previousDestination := ""
	for _, input := range inputs {
		if input.destination == previousDestination {
			return nil, nil, errs.New(errs.KindNameConflict, "Environment materialization destination is duplicated")
		}
		content := input.content
		var err error
		if input.resolve {
			content, err = service.materials.ResolveTaskMaterializationSource(ctx, environmentID, input.source)
			if err != nil {
				clear(content)
				return nil, nil, err
			}
		}
		digest := sha256.Sum256(content)
		reference := etcd.TaskMaterializationRecord{
			StepID:            allocate(ids.KindStep, "materialization-step/"+input.destination),
			MaterializationID: allocate(ids.KindConfig, "materialization/"+input.destination),
			EnvironmentID:     environmentID, Destination: input.destination,
			ServiceID: input.serviceID, ServiceName: input.serviceName,
			OutputKind: input.outputKind, UID: input.uid, GID: input.gid, Mode: uint32(input.mode),
			Length: uint64(len(content)), SHA256: hex.EncodeToString(digest[:]), Source: input.source,
		}
		clear(content)
		step, err := controller.BuildTaskMaterializationStep(
			reference,
			artifactID,
			uint32(environmentBlueprintTimeoutSeconds),
		)
		if err != nil {
			return nil, nil, err
		}
		references = append(references, reference)
		steps = append(steps, step)
		previousDestination = input.destination
	}
	sort.Slice(references, func(left int, right int) bool {
		return references[left].StepID < references[right].StepID
	})
	return references, steps, nil
}

func prepareEnvironmentBlueprintRouteChanges(
	environmentID string,
	desired []core.Route,
	current []etcd.Versioned[etcd.RouteRecord],
) ([]etcd.EnvironmentBlueprintRouteChange, error) {
	currentByID := make(map[string]etcd.Versioned[etcd.RouteRecord], len(current))
	for _, route := range current {
		if route.Record.EnvironmentID != environmentID || route.Record.Desired.ID == "" {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state is inconsistent")
		}
		if _, duplicate := currentByID[route.Record.Desired.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "durable Blueprint Route state repeats an id")
		}
		currentByID[route.Record.Desired.ID] = route
	}
	changes := make([]etcd.EnvironmentBlueprintRouteChange, 0, len(desired))
	for _, next := range desired {
		if existing, found := currentByID[next.ID]; found {
			replacement, err := etcd.ReplaceRouteDesired(existing.Record, next)
			if err != nil {
				return nil, err
			}
			currentCopy := existing
			changes = append(changes, etcd.EnvironmentBlueprintRouteChange{
				Current: &currentCopy,
				Record:  replacement,
			})
			delete(currentByID, next.ID)
			continue
		}
		record, err := etcd.NewRouteRecord(environmentID, next)
		if err != nil {
			return nil, err
		}
		changes = append(changes, etcd.EnvironmentBlueprintRouteChange{Record: record})
	}
	if len(currentByID) != 0 {
		return nil, errs.New(
			errs.KindResourceInUse,
			"Blueprint omits an existing Route; remove it explicitly before apply",
		)
	}
	return changes, nil
}

func environmentBlueprintState(
	environmentID string,
	head etcd.Versioned[etcd.EnvironmentBlueprintHead],
	hasHead bool,
	projection etcd.Versioned[etcd.EnvironmentComposeProjection],
	hasProjection bool,
) (int64, controller.ComposeIdentitySnapshot, uint64, error) {
	if hasHead != hasProjection {
		return 0, controller.ComposeIdentitySnapshot{}, 0, errs.New(
			errs.KindInternal,
			"Environment desired-state pointers are inconsistent",
		)
	}
	if !hasHead {
		return 0, controller.ComposeIdentitySnapshot{}, 1, nil
	}
	if head.Record.EnvironmentID != environmentID || projection.Record.EnvironmentID != environmentID ||
		head.Record.RevisionID != projection.Record.RevisionID || head.Revision <= 0 ||
		projection.Revision != head.Revision || projection.Record.RenderGeneration == math.MaxUint64 {
		return 0, controller.ComposeIdentitySnapshot{}, 0, errs.New(
			errs.KindInternal,
			"Environment desired-state pointers are corrupt",
		)
	}
	snapshot, err := authoredComposeIdentitySnapshot(projection.Record)
	if err != nil {
		return 0, controller.ComposeIdentitySnapshot{}, 0, err
	}
	return head.Revision, snapshot, projection.Record.RenderGeneration + 1, nil
}

func authoredComposeIdentitySnapshot(
	projection etcd.EnvironmentComposeProjection,
) (controller.ComposeIdentitySnapshot, error) {
	project, err := controller.LoadNormalizedEnvironmentProject(context.Background(), projection)
	if err != nil {
		return controller.ComposeIdentitySnapshot{}, err
	}
	authoredNames := make(map[string]struct{}, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		authoredNames[name] = struct{}{}
	}
	for name := range project.DisabledServices {
		authoredNames[name] = struct{}{}
	}
	snapshot := controller.ComposeIdentitySnapshot{
		Services: make([]controller.ComposeResourceIdentity, len(projection.DesiredServices)),
		Networks: make([]controller.ComposeResourceIdentity, len(projection.DesiredZones)),
		Volumes:  make([]controller.ComposeResourceIdentity, len(projection.Volumes)),
	}
	seenServiceIDs := make(map[string]string, len(projection.DesiredServices))
	seenServiceNames := make(map[string]string, len(projection.DesiredServices))
	for index, service := range projection.DesiredServices {
		if service.EnvironmentID != projection.EnvironmentID ||
			ids.Validate(ids.KindService, service.Desired.ID) != nil ||
			service.Desired.Name == "" {
			return controller.ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection has an invalid authored Service identity",
			)
		}
		if name, duplicate := seenServiceIDs[service.Desired.ID]; duplicate && name != service.Desired.Name {
			return controller.ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection repeats an authored Service id",
			)
		}
		if serviceID, duplicate := seenServiceNames[service.Desired.Name]; duplicate &&
			serviceID != service.Desired.ID {
			return controller.ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection repeats an authored Service name",
			)
		}
		if _, authored := authoredNames[service.Desired.Name]; !authored {
			return controller.ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired Service is absent from normalized Compose",
			)
		}
		delete(authoredNames, service.Desired.Name)
		seenServiceIDs[service.Desired.ID] = service.Desired.Name
		seenServiceNames[service.Desired.Name] = service.Desired.ID
		snapshot.Services[index] = controller.ComposeResourceIdentity{
			ID:   service.Desired.ID,
			Name: service.Desired.Name,
		}
	}
	if len(authoredNames) != 0 {
		return controller.ComposeIdentitySnapshot{}, errs.New(
			errs.KindInternal,
			"Environment normalized Compose has no desired Service identity",
		)
	}
	for index, zone := range projection.DesiredZones {
		if zone.EnvironmentID != projection.EnvironmentID || ids.Validate(ids.KindNetwork, zone.Desired.ID) != nil ||
			zone.Desired.Name == "" {
			return controller.ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection has an invalid Zone identity",
			)
		}
		snapshot.Networks[index] = controller.ComposeResourceIdentity{ID: zone.Desired.ID, Name: zone.Desired.Name}
	}
	for index, volume := range projection.Volumes {
		if ids.Validate(ids.KindVolume, volume.ID) != nil || volume.Key == "" {
			return controller.ComposeIdentitySnapshot{}, errs.New(
				errs.KindInternal,
				"Environment desired projection has an invalid Volume identity",
			)
		}
		snapshot.Volumes[index] = controller.ComposeResourceIdentity{ID: volume.ID, Name: volume.Key}
	}
	return snapshot, nil
}

func environmentBlueprintServiceNames(project *composetypes.Project) map[string]struct{} {
	names := make(map[string]struct{}, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		names[name] = struct{}{}
	}
	for name := range project.DisabledServices {
		names[name] = struct{}{}
	}
	return names
}

func preserveEnvironmentBlueprintServiceExtensions(
	submitted map[string]core.ServiceExtensionSpec,
	submittedServiceNames map[string]struct{},
	previous []controller.ComposeResourceIdentity,
	previousExtensions map[string]core.ServiceExtensionSpec,
) (map[string]core.ServiceExtensionSpec, error) {
	result := cloneEnvironmentBlueprintServiceExtensions(submitted)
	for _, identity := range previous {
		if _, submittedNow := submittedServiceNames[identity.Name]; submittedNow {
			continue
		}
		if extension, exists := previousExtensions[identity.Name]; exists {
			result[identity.Name] = cloneEnvironmentBlueprintServiceExtension(extension)
		}
	}
	return result, nil
}

func cloneEnvironmentBlueprintServiceExtensions(
	source map[string]core.ServiceExtensionSpec,
) map[string]core.ServiceExtensionSpec {
	result := make(map[string]core.ServiceExtensionSpec, len(source))
	for name, extension := range source {
		result[name] = cloneEnvironmentBlueprintServiceExtension(extension)
	}
	return result
}

func cloneEnvironmentBlueprintServiceExtension(extension core.ServiceExtensionSpec) core.ServiceExtensionSpec {
	clone := extension
	if extension.Release != nil {
		release := *extension.Release
		clone.Release = &release
	}
	if extension.DependsOn != nil {
		clone.DependsOn = make(map[string]core.ServiceDependency, len(extension.DependsOn))
		for dependency, decision := range extension.DependsOn {
			decision.Phases = append([]core.ServiceDependencyPhase(nil), decision.Phases...)
			clone.DependsOn[dependency] = decision
		}
	}
	return clone
}

func preserveEnvironmentBlueprintResources(
	project *composetypes.Project,
	prior *composetypes.Project,
	previous controller.ComposeIdentitySnapshot,
	previousVolumes []etcd.EnvironmentVolumeIdentity,
	hasPrevious bool,
) error {
	if project == nil || (hasPrevious && prior == nil) {
		return errs.New(errs.KindInternal, "Blueprint Compose project is missing")
	}
	if !hasPrevious {
		return nil
	}
	if project.Services == nil {
		project.Services = make(composetypes.Services, len(previous.Services))
	}
	for _, identity := range previous.Services {
		if _, authored := project.Services[identity.Name]; authored {
			continue
		}
		if _, disabled := project.DisabledServices[identity.Name]; disabled {
			continue
		}
		config, active := prior.Services[identity.Name]
		disabled, profileDisabled := prior.DisabledServices[identity.Name]
		if active && profileDisabled {
			return errs.New(errs.KindResourceInUse, "Blueprint cannot unambiguously preserve an omitted Service")
		}
		if active {
			config.Name = identity.Name
			project.Services[identity.Name] = config
			continue
		}
		if profileDisabled {
			if project.DisabledServices == nil {
				project.DisabledServices = make(composetypes.Services, len(previous.Services))
			}
			disabled.Name = identity.Name
			project.DisabledServices[identity.Name] = disabled
			continue
		}
		return errs.New(errs.KindResourceInUse, "Blueprint cannot preserve an omitted Service")
	}
	if project.Networks == nil {
		project.Networks = make(composetypes.Networks, len(previous.Networks))
	}
	for _, identity := range previous.Networks {
		if _, authored := project.Networks[identity.Name]; authored {
			continue
		}
		network, found := prior.Networks[identity.Name]
		if !found {
			return errs.New(errs.KindResourceInUse, "Blueprint cannot preserve an omitted Zone")
		}
		project.Networks[identity.Name] = network
	}
	if len(prior.Configs) != 0 {
		if project.Configs == nil {
			project.Configs = make(composetypes.Configs, len(prior.Configs))
		}
		for name, config := range prior.Configs {
			if _, authored := project.Configs[name]; !authored {
				project.Configs[name] = config
			}
		}
	}
	if len(prior.Secrets) != 0 {
		if project.Secrets == nil {
			project.Secrets = make(composetypes.Secrets, len(prior.Secrets))
		}
		for name, secret := range prior.Secrets {
			if _, authored := project.Secrets[name]; !authored {
				project.Secrets[name] = secret
			}
		}
	}
	if project.Volumes == nil {
		project.Volumes = make(composetypes.Volumes, len(previousVolumes))
	}
	for _, volume := range previousVolumes {
		if _, authored := project.Volumes[volume.Key]; authored {
			continue
		}
		config, found := prior.Volumes[volume.Key]
		if !found {
			return errs.New(errs.KindResourceInUse, "Blueprint cannot preserve an omitted Volume")
		}
		if config.Extensions == nil {
			config.Extensions = make(composetypes.Extensions)
		}
		config.Extensions["x-gp-slug"] = volume.Slug
		project.Volumes[volume.Key] = config
	}
	return nil
}

func preserveEnvironmentBlueprintRoutes(
	specs []core.RouteSpec,
	services []core.Service,
	current []etcd.Versioned[etcd.RouteRecord],
) ([]core.RouteSpec, error) {
	serviceByID := make(map[string]string, len(services))
	for _, service := range services {
		serviceByID[service.ID] = service.Name
	}
	retained := make(map[string]struct{}, len(specs)+len(current))
	for _, spec := range specs {
		path := spec.Path
		if path == "" {
			path = "/"
		}
		retained[spec.Hostname+"\x00"+path] = struct{}{}
	}
	result := append([]core.RouteSpec(nil), specs...)
	for _, versioned := range current {
		route := versioned.Record.Desired
		serviceName, exists := serviceByID[route.TargetServiceID]
		if !exists {
			return nil, errs.New(errs.KindInternal, "durable Route target Service is not retained")
		}
		path := route.Path
		if path == "" {
			path = "/"
		}
		match := route.Host + "\x00" + path
		if _, authored := retained[match]; authored {
			continue
		}
		result = append(result, core.RouteSpec{
			Hostname: route.Host, Path: path, Target: serviceName,
			TargetPort: route.TargetPort, Exposure: route.Exposure,
		})
		retained[match] = struct{}{}
	}
	return result, nil
}

func managedEnvironmentVolumeIDs(current []controller.ComposeResourceIdentity) []string {
	result := make([]string, 0, len(current))
	for _, volume := range current {
		result = append(result, volume.ID)
	}
	sort.Strings(result)
	return result
}

func environmentBlueprintVolumeSlugs(
	project *composetypes.Project,
	previous etcd.EnvironmentComposeProjection,
	hasPrevious bool,
) (map[string]string, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Compose project is missing")
	}
	previousByKey := make(map[string]string, len(previous.Volumes))
	if hasPrevious {
		for _, volume := range previous.Volumes {
			previousByKey[volume.Key] = volume.Slug
		}
	}
	result := make(map[string]string, len(project.Volumes))
	seenSlugs := make(map[string]struct{}, len(project.Volumes))
	for key, volume := range project.Volumes {
		volumeSlug := ""
		if authored, exists := volume.Extensions["x-gp-slug"]; exists {
			var ok bool
			volumeSlug, ok = authored.(string)
			if !ok {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint volume x-gp-slug must be a string")
			}
		} else if retained, exists := previousByKey[key]; exists {
			volumeSlug = retained
		} else {
			volumeSlug = key
		}
		if err := slug.Validate("Blueprint volume x-gp-slug", volumeSlug); err != nil {
			return nil, err
		}
		if _, duplicate := seenSlugs[volumeSlug]; duplicate {
			return nil, errs.New(errs.KindNameConflict, "Blueprint volume slug is already in use")
		}
		seenSlugs[volumeSlug] = struct{}{}
		result[key] = volumeSlug
	}
	return result, nil
}

func environmentBlueprintVolumeMounts(
	project *composetypes.Project,
	identities controller.ComposeIdentitySnapshot,
) ([]etcd.EnvironmentServiceVolumeMount, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Compose project is missing")
	}
	services := make(map[string]string, len(identities.Services))
	for _, service := range identities.Services {
		services[service.Name] = service.ID
	}
	volumes := make(map[string]string, len(identities.Volumes))
	for _, volume := range identities.Volumes {
		volumes[volume.Name] = volume.ID
	}
	mounts := make([]etcd.EnvironmentServiceVolumeMount, 0)
	for serviceName, service := range project.Services {
		serviceID, exists := services[serviceName]
		if !exists {
			return nil, errs.New(errs.KindInternal, "Blueprint Service identity is missing")
		}
		seenTargets := make(map[string]struct{}, len(service.Volumes))
		for _, mount := range service.Volumes {
			if mount.Type != composetypes.VolumeTypeVolume {
				continue
			}
			volumeID, exists := volumes[mount.Source]
			if !exists {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Service references an unknown managed Volume",
				)
			}
			if mount.Target == "" || !path.IsAbs(mount.Target) || path.Clean(mount.Target) != mount.Target {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Volume mount target must be a clean absolute path",
				)
			}
			if _, duplicate := seenTargets[mount.Target]; duplicate {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint Service repeats a Volume mount target")
			}
			seenTargets[mount.Target] = struct{}{}
			mounts = append(mounts, etcd.EnvironmentServiceVolumeMount{
				ServiceID: serviceID, VolumeID: volumeID, Target: mount.Target, ReadOnly: mount.ReadOnly,
			})
		}
	}
	sort.Slice(mounts, func(left int, right int) bool {
		leftKey := mounts[left].ServiceID + "\x00" + mounts[left].Target
		rightKey := mounts[right].ServiceID + "\x00" + mounts[right].Target
		return leftKey < rightKey
	})
	return mounts, nil
}
