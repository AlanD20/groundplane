package blueprint

import (
	"context"
	"crypto/rand"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/attachments"
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
	environmentBlueprintRoute           = "/environments/{id}/blueprint"
	maximumEnvironmentBlueprintAttempts = 3
)

type Service struct {
	volumeRoot        string
	environmentPool   netip.Prefix
	repository        environmentBlueprintRepository
	idempotency       *desiredrevision.Idempotency
	materials         materializationResolver
	releaseGroups     *releasegroup.ReleaseGroupBlueprintPlanner
	blueprintReleases *blueprintrelease.Service
	entryGeneration   *entrygeneration.EntryGenerationService
	attachFacts       *attachments.FactService
	componentCatalog  []componentrender.EnvironmentComponentRegistration
	backups           environmentBlueprintBackupRepository
	backupKeys        environmentBlueprintBackupKeyFactory
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
	backups environmentBlueprintBackupRepository,
	backupKeys environmentBlueprintBackupKeyFactory,
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
		blueprintReleases == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint service is not configured")
	}
	if _, err := taskplanning.NewTaskPlanResolver(volumeRoot, componentCatalog); err != nil {
		return nil, err
	}
	return &Service{
		volumeRoot: volumeRoot, environmentPool: parsedEnvironmentPool, repository: repository, idempotency: idempotency,
		materials: materials, releaseGroups: releaseGroups, blueprintReleases: blueprintReleases,
		entryGeneration:  entryGeneration,
		attachFacts:      attachFacts,
		componentCatalog: componentrender.CloneEnvironmentComponentCatalog(componentCatalog),
		backups:          backups,
		backupKeys:       backupKeys,
		random:           rand.Reader, now: time.Now,
	}, nil
}

func (service *Service) ApplyBlueprint(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.applyBlueprint(ctx, environmentID, environmentID, bundle, expectedRevision, idempotencyKey, false)
}

func (service *Service) ApplyComponentBlueprint(
	ctx context.Context,
	environmentID string,
	componentID string,
	bundle core.BlueprintBundle,
	expectedRevision, idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || expectedRevision == "" {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Component id or Blueprint revision is invalid",
		)
	}
	return service.applyBlueprint(ctx, environmentID, environmentID, bundle, expectedRevision, idempotencyKey, true)
}

func (service *Service) applyBlueprint(
	ctx context.Context,
	environmentID string,
	taskTarget string,
	bundle core.BlueprintBundle,
	expectedRevision string,
	idempotencyKey string,
	preserveRoutes bool,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Environment id is invalid")
	}
	if err := bundle.Validate(); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Blueprint bundle is invalid")
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
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment Blueprint retry bound was not enforced")
}
