package blueprint

import (
	"context"
	"crypto/rand"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	componentrender "github.com/AlanD20/groundplane/internal/controller/componentrender"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	releasegroup "github.com/AlanD20/groundplane/internal/controller/releasegroup"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"net/netip"
	"time"
)

const (
	environmentBlueprintRoute = "/environments/{id}/blueprint"
)

type Service struct {
	acceptedActions   *acceptedaction.Submitter
	volumeRoot        string
	environmentPool   netip.Prefix
	repository        environmentBlueprintRepository
	idempotency       *desiredrevision.Idempotency
	materials         materializationResolver
	releaseGroups     *releasegroup.ReleaseGroupBlueprintPlanner
	blueprintReleases *blueprintrelease.Service
	configuration     *taskplanning.ComponentConfigurationCapture
	entryGeneration   *entrygeneration.EntryGenerationService
	attachFacts       *attachments.FactService
	componentCatalog  []componentrender.EnvironmentComponentRegistration
	workAdmission     *blueprintcoordinator.Admission
	backups           environmentBlueprintBackupRepository
	backupKeys        environmentBlueprintBackupKeyFactory
	executionTimeout  time.Duration
	random            io.Reader
	now               func() time.Time
}

func NewService(
	volumeRoot string,
	environmentPool string,
	repository environmentBlueprintRepository,
	idempotency *desiredrevision.Idempotency,
	materials materializationResolver,
	releaseGroups *releasegroup.ReleaseGroupBlueprintPlanner,
	blueprintReleases *blueprintrelease.Service,
	entryGeneration *entrygeneration.EntryGenerationService,
	attachFacts *attachments.FactService,
	componentCatalog []componentrender.EnvironmentComponentRegistration,
	configuration *taskplanning.ComponentConfigurationCapture,
	workAdmission *blueprintcoordinator.Admission,
	backups environmentBlueprintBackupRepository,
	backupKeys environmentBlueprintBackupKeyFactory,
	executionTimeout time.Duration,
	acceptedActions *acceptedaction.Submitter,
) (*Service, error) {
	parsedEnvironmentPool, poolErr := netip.ParsePrefix(environmentPool)
	if poolErr != nil || !parsedEnvironmentPool.Addr().Is4() ||
		parsedEnvironmentPool != parsedEnvironmentPool.Masked() ||
		repository == nil ||
		idempotency == nil ||
		materials == nil ||
		releaseGroups == nil ||
		entryGeneration == nil ||
		attachFacts == nil ||
		blueprintReleases == nil || configuration == nil || workAdmission == nil || acceptedActions == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	if executionTimeout < 40*time.Minute || executionTimeout > 24*time.Hour || executionTimeout%time.Second != 0 {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint execution timeout must be an integral duration from 40m through 24h",
		)
	}
	return &Service{
		acceptedActions: acceptedActions, volumeRoot: volumeRoot, environmentPool: parsedEnvironmentPool, repository: repository, idempotency: idempotency,
		materials: materials, releaseGroups: releaseGroups, blueprintReleases: blueprintReleases,
		configuration:    configuration,
		workAdmission:    workAdmission,
		entryGeneration:  entryGeneration,
		attachFacts:      attachFacts,
		componentCatalog: componentrender.CloneEnvironmentComponentCatalog(componentCatalog),
		backups:          backups,
		backupKeys:       backupKeys,
		executionTimeout: executionTimeout,
		random:           rand.Reader, now: time.Now,
	}, nil
}

func (service *Service) ApplyBlueprint(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
	after []string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.applyBlueprint(ctx, environmentID, bundle, expectedRevision, idempotencyKey, false, after)
}

func (service *Service) applyBlueprint(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
	preserveRoutes bool,
	after []string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Environment Blueprint context is required",
		)
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Environment id is invalid")
	}
	if err := bundle.Validate(); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint bundle is invalid",
		)
	}
	return service.acceptBlueprint(ctx, environmentID, bundle, expectedRevision, idempotencyKey, preserveRoutes, after)
}
