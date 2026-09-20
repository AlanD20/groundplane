package blueprint

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
	"time"
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
	ListZones(context.Context, string, etcd.PageRequest) (etcd.Page[zonerecord.Record], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	GetService(context.Context, string) (etcd.Versioned[etcd.ServiceRecord], error)
	ResolveBackingProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	ResolveEnvironment(context.Context, string, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	ListRoutes(context.Context, string, etcd.PageRequest) (etcd.Page[routerecord.Record], error)
	ListEntries(context.Context, string, etcd.PageRequest) (etcd.Page[entryrecord.Record], error)
	BlueprintEntryValueGenerationExists(context.Context, entryrecord.Record) (bool, error)
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

type durableRepository struct {
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

func NewRepository(
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
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	return repository.desired.ClaimEnvironmentBlueprintStage(ctx, request)
}
func (repository *durableRepository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	return repository.desired.StageEnvironmentBlueprintRevision(ctx, request)
}
func (repository *durableRepository) AbandonEnvironmentBlueprintStage(
	ctx context.Context,
	claim etcd.EnvironmentBlueprintStageClaim,
) error {
	return repository.desired.AbandonEnvironmentBlueprintStage(ctx, claim)
}
func (repository *durableRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[entryrecord.Record], error) {
	return repository.entries.ListEntries(ctx, environmentID, request)
}
func (repository *durableRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.AttachRecord], error) {
	return repository.attaches.ListAttaches(ctx, environmentID, request)
}
func (repository *durableRepository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ComponentRecord], error) {
	return repository.components.ListEnvironmentComponents(ctx, environmentID, request)
}
func (repository *durableRepository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[zonerecord.Record], error) {
	return repository.zones.ListZones(ctx, environmentID, request)
}

func (repository *durableRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *durableRepository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[routerecord.Record], error) {
	return repository.routes.ListRoutes(ctx, environmentID, request)
}

func (repository *durableRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.ScriptRecord], error) {
	return repository.scripts.ListScripts(ctx, environmentID, request)
}

func (repository *durableRepository) PrepareBlueprintScriptPublication(
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

func (repository *durableRepository) PublishEnvironmentBlueprintDesiredRevision(
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
