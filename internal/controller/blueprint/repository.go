package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	entryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentBlueprintRepository interface {
	GetBlueprintPrerequisiteAuthority(context.Context, string, string, int64,
	) (etcdstore.Versioned[etcd.BlueprintPrerequisiteAuthority], bool, error)
	ValidateAgentProxyPublication(
		context.Context,
		workqueue.View,
		componentplanning.ProxyServicePreflight,
		workqueue.Change,
	) error
	LoadComponentWorkConfiguration(
		context.Context,
		runtimeconfiguration.Reference,
	) (runtimeconfiguration.Snapshot, error)
	PrepareComponentWorkSecretPins(
		context.Context,
		workqueue.View,
		string,
		runtimeconfiguration.WorkCapture,
	) (tasksecretpins.Prepared, error)
	PrepareComponentWorkConfiguration(
		context.Context,
		workqueue.View,
		string,
		uint64,
		[]taskmaterialization.Record,
	) (runtimeconfiguration.WorkCapture, error)
	ComponentWorkConfigurationConditions(
		context.Context,
		workqueue.View,
		runtimeconfiguration.WorkCapture,
	) ([]etcdstore.Condition, error)
	PrepareBlueprintDisabledComponent(context.Context, workqueue.View, blueprintunits.Unit,
		etcd.BlueprintDisabledComponentSelection, *componentplanning.ProxyServicePreflight, workqueue.Change, time.Time,
	) (workqueue.Change, error)
	LoadBlueprintAdmissionSnapshot(context.Context, string, int64) (blueprintunits.Snapshot, error)
	StageBlueprintProxyCandidateInput(context.Context, workqueue.View,
		projectionrecord.EnvironmentComposeProjection, int64,
	) (etcdstore.Versioned[componentplanning.ProxyCandidateInputReference], error)
	StageBlueprintDisabledProxyCandidateInput(context.Context, workqueue.View,
		componentrecord.Record, projectionrecord.EnvironmentComposeProjection, int64,
	) (etcdstore.Versioned[componentplanning.ProxyCandidateInputReference], error)
	ReadZoneDNSActivation(context.Context, []string) (etcd.ZoneDNSActivation, error)
	GetTenant(context.Context, string) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetEnvironmentBlueprintHead(
		context.Context,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintHead], bool, error)
	GetEnvironmentBlueprintRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[blueprints.EnvironmentBlueprintRevision], bool, error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetEnvironmentComposeProjectionRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	GetEnvironmentDesiredInput(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
	GetEnvironmentDesiredInputRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
	GetEnvironmentOwnedIdentities(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error)
	GetEnvironmentOwnedIdentitiesRevision(
		context.Context,
		string,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentOwnedIdentities], bool, error)
	FindEnvironmentVolume(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], projectionrecord.EnvironmentVolumeIdentity, error)
	ListZones(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[zonerecord.Record], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetAuthoredServiceRemovalSource(
		context.Context,
		string,
		string,
		string,
	) (etcd.AuthoredServiceRemovalSource, error)
	GetServiceRemovalRuntime(
		context.Context,
		string,
		string,
		int64,
	) (etcdstore.Versioned[serviceruntimerecord.Record], error)
	GetEntry(context.Context, string) (etcdstore.Versioned[entryrecord.Record], error)
	GetEnvironmentAppliedComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
	ResolveBackingProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	ResolveEnvironment(context.Context, string, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	ListRoutes(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[routerecord.Record], error)
	ListEntries(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[entryrecord.Record], error)
	BlueprintEntryValueGenerationExists(context.Context, entryrecord.Record) (bool, error)
	CreateBlueprintEntryValueGeneration(context.Context, entryrecord.EntryValueGeneration) error
	BindBlueprintEntryEnvironment(context.Context, string, string) error
	ListAttaches(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[attachrecord.Record], error)
	ListEnvironmentComponents(
		context.Context,
		string,
		etcdstore.PageRequest,
	) (etcdstore.Page[componentrecord.Record], error)
	ReadEnvironmentBlueprintRuntimeComponents(
		context.Context,
		string,
		int64,
	) ([]etcd.BlueprintRuntimeComponent, error)
	PrepareFrozenComponentUnitAuthority(
		context.Context,
		componentplanning.FrozenProjectionAuthorityInput,
		etcdstore.Versioned[componentrecord.Record],
	) (componentplanning.FrozenProjectionAuthority, error)
	PrepareWorkPlacement(context.Context, workqueue.View, etcdstore.Versioned[componentrecord.Record],
		core.Component, componentplanning.FrozenProjectionAuthority) (componentrecord.Record, workqueue.Change, error)
	PrepareFrozenProxyServicePreflight(context.Context, componentplanning.FrozenProjectionAuthority,
		string, core.ComponentKind, []core.Service) (componentplanning.FrozenProjectionAuthority, error)
	ValidateBlueprintProxyServiceSource(context.Context, string, string, string, int64) error
	ClaimEnvironmentBlueprintStage(
		context.Context,
		blueprints.EnvironmentBlueprintStageClaimRequest,
	) (blueprints.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		blueprints.EnvironmentBlueprintStageRequest,
	) (blueprints.EnvironmentBlueprintSeal, error)
	AbandonEnvironmentBlueprintStage(context.Context, blueprints.EnvironmentBlueprintStageClaim) error
	PublishEnvironmentBlueprintAuthoredRevision(
		context.Context,
		etcd.BlueprintAuthoredPublication,
	) (etcdstore.TransactionResult, error)
	SealEnvironmentBlueprintRuntimeProjection(
		context.Context,
		etcd.BlueprintRuntimeProjectionSeal,
	) error
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
func (repository *durableRepository) GetEnvironmentDesiredInput(
	ctx context.Context, environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	return repository.desired.GetEnvironmentDesiredInput(ctx, environmentID)
}
func (repository *durableRepository) CreateBlueprintEntryValueGeneration(
	ctx context.Context,
	generation entryrecord.EntryValueGeneration,
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
func (repository *durableRepository) GetEntry(
	ctx context.Context, entryID string,
) (etcdstore.Versioned[entryrecord.Record], error) {
	return repository.entries.GetEntry(ctx, entryID)
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
func (repository *durableRepository) ReadEnvironmentBlueprintRuntimeComponents(
	ctx context.Context,
	environmentID string,
	revision int64,
) ([]etcd.BlueprintRuntimeComponent, error) {
	return repository.EnvironmentBlueprintRepository.ReadEnvironmentBlueprintRuntimeComponents(
		ctx, environmentID, revision,
	)
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
