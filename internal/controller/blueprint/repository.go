package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
	"time"
)

type environmentBlueprintRepository interface {
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(context.Context, string) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	FindEnvironmentVolume(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error)
	ListZones(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[zonerecord.Record], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	ResolveBackingProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	ResolveEnvironment(context.Context, string, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	ListRoutes(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[routerecord.Record], error)
	ListEntries(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[entryrecord.Record], error)
	BlueprintEntryValueGenerationExists(context.Context, entryrecord.Record) (bool, error)
	CreateBlueprintEntryValueGeneration(context.Context, etcd.EntryValueGeneration) error
	BindBlueprintEntryEnvironment(context.Context, string, string) error
	ListAttaches(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[attachrecord.Record], error)
	ListEnvironmentComponents(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[componentrecord.Record], error)
	PrepareEnvironmentComponentTask(
		context.Context,
		string,
		string,
		[]blueprints.EnvironmentBlueprintZoneChange,
		[]etcd.EnvironmentComponentCandidateInput,
		time.Time,
	) (etcd.ComponentTaskPreparation, error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		blueprints.EnvironmentBlueprintStageClaimRequest,
	) (blueprints.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		blueprints.EnvironmentBlueprintStageRequest,
	) (blueprints.EnvironmentBlueprintSeal, error)
	AbandonEnvironmentBlueprintStage(context.Context, blueprints.EnvironmentBlueprintStageClaim) error
	PublishEnvironmentBlueprintDesiredRevision(
		context.Context,
		netip.Prefix,
		string,
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		int64,
		blueprints.EnvironmentBlueprintStageClaim,
		blueprints.EnvironmentDesiredRevisionIdentity,
		projectionrecord.EnvironmentComposeProjection,
		[]blueprints.EnvironmentBlueprintZoneChange,
		[]blueprints.EnvironmentBlueprintServiceChange,
		[]blueprints.EnvironmentBlueprintRouteChange,
		etcd.ReleaseGroupBlueprintPreparedMutation,
		etcd.ComponentTaskPreparation,
		etcd.BlueprintAttachTaskPreparation,
		etcd.BlueprintBackupPolicyPreparation,
		etcd.BlueprintScriptPublication,
		etcd.BlueprintReleasePublication,
		etcd.BlueprintRequirementGate,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	ListScripts(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[scriptrecord.Record], error)
	PrepareBlueprintScriptPublication(
		context.Context,
		string,
		int64,
		string,
		[]etcdstore.Versioned[scriptrecord.Record],
		[]scriptrecord.Record,
		[]scriptrecord.BodyGenerationRecord,
	) (etcd.BlueprintScriptPublication, error)
}

type durableRepository struct {
	*etcd.EnvironmentBlueprintRepository
	desired    *desiredrevisionstore.Repository
	zones      *etcd.ZoneRepository
	services   *etcd.ServiceRepository
	routes     *etcd.RouteRepository
	entries    *etcd.EntryRepository
	values     *entryvalues.Repository
	attaches   *etcd.AttachRepository
	components *etcd.ComponentRepository
	scripts    *etcd.ScriptRepository
	backups    *etcd.BackupPolicyRepository
	connectors *etcd.ConnectorRepository
}

func NewRepository(
	hierarchy *etcd.EnvironmentBlueprintRepository,
	desired *desiredrevisionstore.Repository,
	zones *etcd.ZoneRepository,
	services *etcd.ServiceRepository,
	routes *etcd.RouteRepository,
	entries *etcd.EntryRepository,
	values *entryvalues.Repository,
	attaches *etcd.AttachRepository,
	components *etcd.ComponentRepository,
	scripts *etcd.ScriptRepository,
	backups *etcd.BackupPolicyRepository,
	connectors *etcd.ConnectorRepository,
) (*durableRepository, error) {
	if hierarchy == nil || desired == nil || zones == nil || services == nil || routes == nil || entries == nil ||
		values == nil ||
		attaches == nil ||
		components == nil ||
		scripts == nil {
		return nil, errs.New(errs.KindInternal, "Environment Blueprint repositories are not configured")
	}
	return &durableRepository{
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
		backups:                        backups,
		connectors:                     connectors,
	}, nil
}
func (repository *durableRepository) BlueprintEntryValueGenerationExists(
	ctx context.Context,
	record entryrecord.Record,
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
func (repository *durableRepository) CreateBlueprintEntryValueGeneration(
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
func (repository *durableRepository) BindBlueprintEntryEnvironment(
	ctx context.Context,
	environmentID string,
	entryID string,
) error {
	return repository.entries.BindBlueprintEntryEnvironment(ctx, environmentID, entryID)
}
func (repository *durableRepository) ResolveBlueprintEntryEnvironment(
	ctx context.Context,
	entryID string,
) (string, bool, error) {
	return repository.entries.ResolveBlueprintEntryEnvironment(ctx, entryID)
}
func (repository *durableRepository) ClaimEnvironmentBlueprintStage(
	ctx context.Context,
	request blueprints.EnvironmentBlueprintStageClaimRequest,
) (blueprints.EnvironmentBlueprintStageClaim, error) {
	return repository.desired.ClaimEnvironmentBlueprintStage(ctx, request)
}
func (repository *durableRepository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request blueprints.EnvironmentBlueprintStageRequest,
) (blueprints.EnvironmentBlueprintSeal, error) {
	return repository.desired.StageEnvironmentBlueprintRevision(ctx, request)
}
func (repository *durableRepository) AbandonEnvironmentBlueprintStage(
	ctx context.Context,
	claim blueprints.EnvironmentBlueprintStageClaim,
) error {
	return repository.desired.AbandonEnvironmentBlueprintStage(ctx, claim)
}
func (repository *durableRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[entryrecord.Record], error) {
	return repository.entries.ListEntries(ctx, environmentID, request)
}
func (repository *durableRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[attachrecord.Record], error) {
	return repository.attaches.ListAttaches(ctx, environmentID, request)
}
func (repository *durableRepository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[componentrecord.Record], error) {
	return repository.components.ListEnvironmentComponents(ctx, environmentID, request)
}
func (repository *durableRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[zonerecord.Record], error) {
	return repository.zones.ListZones(ctx, environmentID, request)
}

func (repository *durableRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[servicerecord.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *durableRepository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[routerecord.Record], error) {
	return repository.routes.ListRoutes(ctx, environmentID, request)
}

func (repository *durableRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[scriptrecord.Record], error) {
	return repository.scripts.ListScripts(ctx, environmentID, request)
}

func (repository *durableRepository) PrepareBlueprintScriptPublication(
	ctx context.Context,
	environmentID string,
	readRevision int64,
	nextGenerationID string,
	current []etcdstore.Versioned[scriptrecord.Record],
	desired []scriptrecord.Record,
	generations []scriptrecord.BodyGenerationRecord,
) (etcd.BlueprintScriptPublication, error) {
	return repository.scripts.PrepareBlueprintScriptPublication(
		ctx, environmentID, readRevision, nextGenerationID, current, desired, generations,
	)
}

func (repository *durableRepository) PublishEnvironmentBlueprintDesiredRevision(
	ctx context.Context,
	environmentPool netip.Prefix,
	desiredNetworkPool string,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64,
	claim blueprints.EnvironmentBlueprintStageClaim,
	revision blueprints.EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	zoneChanges []blueprints.EnvironmentBlueprintZoneChange,
	serviceChanges []blueprints.EnvironmentBlueprintServiceChange,
	routeChanges []blueprints.EnvironmentBlueprintRouteChange,
	releaseGroupPreparation etcd.ReleaseGroupBlueprintPreparedMutation,
	componentPreparation etcd.ComponentTaskPreparation,
	attachPreparation etcd.BlueprintAttachTaskPreparation,
	backupPreparation etcd.BlueprintBackupPolicyPreparation,
	scriptPublication etcd.BlueprintScriptPublication,
	releasePublication etcd.BlueprintReleasePublication,
	requirementGate etcd.BlueprintRequirementGate,
	task etcd.TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.EnvironmentBlueprintRepository.PublishEnvironmentBlueprintDesiredRevision(
		ctx, environmentPool, desiredNetworkPool, project, environment, expectedHeadRevision,
		claim, revision, projection, zoneChanges, serviceChanges, routeChanges,
		releaseGroupPreparation, componentPreparation, attachPreparation,
		backupPreparation,
		scriptPublication, releasePublication, requirementGate, task, marker,
	)
}
