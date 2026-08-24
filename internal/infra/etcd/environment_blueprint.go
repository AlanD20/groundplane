package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentBlueprintMaxFiles      = 64
	environmentBlueprintMaxFileBytes  = 256 * 1024
	environmentBlueprintMaxTotalBytes = 768 * 1024
	environmentBlueprintMaxPathBytes  = 240

	EnvironmentBlueprintRevisionParam = "blueprint_revision_id"
)

var environmentBlueprintInterpolationKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// EnvironmentBlueprintRevision is one immutable, verified desired-state
// input. RevisionID is the Task id that first reconciles it, so queued work can
// never be retargeted when a later apply advances the current pointer.
type EnvironmentBlueprintRevision struct {
	EnvironmentID  string
	RevisionID     string
	RootPath       string
	ComposeSources []string
	Interpolation  map[string]string
	Files          []EnvironmentBlueprintFile
	CreatedAt      time.Time
}

type EnvironmentBlueprintFile struct {
	Path    string
	Content []byte
}

type EnvironmentBlueprintHead struct {
	EnvironmentID string
	RevisionID    string
}

// EnvironmentBlueprintZoneChange is one immutable existing Zone fence or one
// new Zone and subnet reservation committed with the Blueprint head.
type EnvironmentBlueprintZoneChange struct {
	Current *Versioned[ZoneRecord]
	Record  ZoneRecord
}

// EnvironmentBlueprintServiceChange is one desired-only Service replacement
// committed with the Blueprint head. Current is nil only when the Blueprint
// first introduces the stable Service id.
type EnvironmentBlueprintServiceChange struct {
	Current *Versioned[ServiceRecord]
	Record  ServiceRecord
}

// EnvironmentBlueprintRouteChange is one exposure-only Route replacement or
// one new stable Route committed with its target Service desired state.
type EnvironmentBlueprintRouteChange struct {
	Current *Versioned[RouteRecord]
	Record  RouteRecord
}

type environmentBlueprintManifest struct {
	EnvironmentID  string                             `json:"environment_id"`
	RevisionID     string                             `json:"revision_id"`
	RootPath       string                             `json:"root"`
	ComposeSources []string                           `json:"compose_sources"`
	Interpolation  map[string]string                  `json:"interpolation"`
	Files          []environmentBlueprintManifestFile `json:"files"`
	CreatedAt      string                             `json:"created_at"`
}

type environmentBlueprintManifestFile struct {
	Path   string `json:"path"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

func environmentBlueprintHeadKey(environmentID string) string {
	return "/v1/records/environment-blueprints/" + environmentID + "/current"
}

func environmentBlueprintRevisionsPrefix(environmentID string) string {
	return "/v1/records/environment-blueprints/" + environmentID + "/revisions/"
}

func environmentBlueprintRevisionPrefix(environmentID string, revisionID string) string {
	return environmentBlueprintRevisionsPrefix(environmentID) + revisionID + "/"
}

func environmentBlueprintManifestKey(environmentID string, revisionID string) string {
	return environmentBlueprintRevisionPrefix(environmentID, revisionID) + "manifest"
}

func environmentBlueprintFileKey(environmentID string, revisionID string, index int) string {
	return environmentBlueprintRevisionPrefix(environmentID, revisionID) + fmt.Sprintf("files/%06d", index+1)
}

// GetEnvironmentBlueprintHead returns the current immutable revision pointer.
// A missing head is normal before an Environment's first successful apply.
func (repository *HierarchyRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (Versioned[EnvironmentBlueprintHead], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	result, err := repository.store.Get(ctx, environmentBlueprintHeadKey(environmentID))
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if result.Entry == nil {
		return Versioned[EnvironmentBlueprintHead]{ReadRevision: result.ReadRevision}, false, nil
	}
	revisionID, err := decodeTaskReference(result.Entry.Value)
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	return Versioned[EnvironmentBlueprintHead]{
		Record:   EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: revisionID},
		Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

// GetEnvironmentBlueprintRevision reconstructs verified file bytes at the
// manifest's pinned MVCC revision. Missing immutable state is reported as not
// found; mismatched bytes or metadata are durable corruption.
func (repository *HierarchyRepository) GetEnvironmentBlueprintRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (Versioned[EnvironmentBlueprintRevision], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := validateID(ids.KindTask, revisionID); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	manifestResult, err := repository.store.Get(ctx, environmentBlueprintManifestKey(environmentID, revisionID))
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if manifestResult.Entry == nil {
		return Versioned[EnvironmentBlueprintRevision]{ReadRevision: manifestResult.ReadRevision}, false, nil
	}
	manifest, err := decodeEnvironmentBlueprintManifest(manifestResult.Entry.Value)
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if manifest.EnvironmentID != environmentID || manifest.RevisionID != revisionID {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	createdAt, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	keys := make([]string, len(manifest.Files))
	for index := range manifest.Files {
		keys[index] = environmentBlueprintFileKey(environmentID, revisionID, index)
	}
	fileResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: keys, Revision: manifestResult.ReadRevision,
	})
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if len(fileResult.Values) != len(keys) || fileResult.ReadRevision != manifestResult.ReadRevision {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	revision := EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: revisionID, RootPath: manifest.RootPath,
		ComposeSources: append([]string(nil), manifest.ComposeSources...),
		Interpolation:  cloneEnvironmentBlueprintInterpolation(manifest.Interpolation),
		Files:          make([]EnvironmentBlueprintFile, len(manifest.Files)),
		CreatedAt:      createdAt,
	}
	for index, metadata := range manifest.Files {
		value := fileResult.Values[index]
		if value == nil || len(value.Value) != metadata.Size {
			return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
		}
		digest := sha256.Sum256(value.Value)
		if hex.EncodeToString(digest[:]) != metadata.SHA256 {
			return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
		}
		revision.Files[index] = EnvironmentBlueprintFile{
			Path: metadata.Path, Content: append([]byte(nil), value.Value...),
		}
	}
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	return Versioned[EnvironmentBlueprintRevision]{
		Record: revision, Revision: manifestResult.Entry.ModRevision,
		ReadRevision: manifestResult.ReadRevision,
	}, true, nil
}

// ApplyEnvironmentBlueprintWithTask atomically publishes immutable desired
// input, advances the current pointer, and enqueues its sealed reconcile Task.
func (repository *HierarchyRepository) ApplyEnvironmentBlueprintWithTask(
	ctx context.Context,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedHeadRevision int64,
	revision EnvironmentBlueprintRevision,
	projection EnvironmentComposeProjection,
	zoneChanges []EnvironmentBlueprintZoneChange,
	serviceChanges []EnvironmentBlueprintServiceChange,
	routeChanges []EnvironmentBlueprintRouteChange,
	componentPreparation ComponentTaskPreparation,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.Record.Kind != ProjectKindTenant || project.Revision <= 0 || environment.Revision <= 0 ||
		project.ReadRevision < project.Revision || environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID ||
		environment.Record.ProvisioningState != EnvironmentProvisioningReady || expectedHeadRevision < 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"Environment is not ready for Blueprint apply",
		)
	}
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	previousProjection, hasPreviousProjection, err := repository.GetEnvironmentComposeProjection(
		ctx,
		environment.Record.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if (expectedHeadRevision == 0 && hasPreviousProjection) ||
		(expectedHeadRevision > 0 && (!hasPreviousProjection || previousProjection.Revision != expectedHeadRevision)) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	if projection.EnvironmentID != environment.Record.ID || projection.BlueprintRevisionID != revision.RevisionID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment Compose projection does not identify its Blueprint revision",
		)
	}
	if err := validateEnvironmentComposeProjectionAdvance(
		previousProjection.Record,
		hasPreviousProjection,
		projection,
	); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	preparedZones, err := repository.prepareEnvironmentBlueprintZoneChanges(
		ctx,
		environment,
		projection,
		zoneChanges,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearPreparedEnvironmentBlueprintZones(preparedZones)
	preparedServices, err := repository.prepareEnvironmentBlueprintServiceChanges(
		ctx,
		environment,
		projection,
		serviceChanges,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearPreparedEnvironmentBlueprintServices(preparedServices)
	preparedRoutes, err := repository.prepareEnvironmentBlueprintRouteChanges(
		ctx,
		environment,
		serviceChanges,
		routeChanges,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearPreparedEnvironmentBlueprintRoutes(preparedRoutes)
	if revision.EnvironmentID != environment.Record.ID || revision.RevisionID != task.ID ||
		!revision.CreatedAt.Equal(task.CreatedAt) || task.Type != TaskUpdate ||
		task.Target != environment.Record.ID || task.Status != TaskStatusPending ||
		task.Params[EnvironmentBlueprintRevisionParam] != revision.RevisionID ||
		task.Params[TaskMaterializationEnvironmentParam] != environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Blueprint apply Task does not own its immutable revision",
		)
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Blueprint apply marker does not match its Environment-scoped Task",
		)
	}
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	manifestValue, err := encodeEnvironmentBlueprintManifest(revision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(manifestValue)
	projectionValue, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(projectionValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: environmentBlueprintManifestKey(revision.EnvironmentID, revision.RevisionID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  MutationPut,
			Key:   environmentBlueprintManifestKey(revision.EnvironmentID, revision.RevisionID),
			Value: manifestValue,
		},
	}
	for index, file := range revision.Files {
		key := environmentBlueprintFileKey(revision.EnvironmentID, revision.RevisionID, index)
		conditions = append(conditions, Condition{Key: key})
		mutations = append(
			mutations,
			Mutation{Type: MutationPut, Key: key, Value: append([]byte(nil), file.Content...)},
		)
	}
	conditions = append(conditions,
		Condition{Key: environmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
		Condition{Key: environmentComposeProjectionKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
		Condition{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		Condition{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		Condition{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		Condition{Key: deletionTombstoneKey("project", project.Record.ID)},
		Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)},
	)
	mutations = append(
		mutations,
		Mutation{Type: MutationPut, Key: environmentBlueprintHeadKey(revision.EnvironmentID), Value: reference},
		Mutation{
			Type:  MutationPut,
			Key:   environmentComposeProjectionKey(revision.EnvironmentID),
			Value: projectionValue,
		},
	)
	for _, zone := range preparedZones.changes {
		primaryCondition := Condition{Key: zoneKey(zone.change.Record.Desired.ID)}
		nameCondition := Condition{
			Key: zoneNameKey(zone.change.Record.EnvironmentID, zone.change.Record.Desired.Name),
		}
		ownerCondition := Condition{
			Key: zoneOwnerKey(zone.change.Record.EnvironmentID, zone.change.Record.Desired.ID),
		}
		if zone.change.Current != nil {
			primaryCondition.ModRevision = zone.change.Current.Revision
			nameCondition.ModRevision = zone.nameRevision
			ownerCondition.ModRevision = zone.ownerRevision
		}
		conditions = append(
			conditions,
			primaryCondition,
			nameCondition,
			ownerCondition,
			Condition{Key: deletionTombstoneKey("zone", zone.change.Record.Desired.ID)},
		)
		if zone.change.Current == nil {
			mutations = append(
				mutations,
				Mutation{Type: MutationPut, Key: zoneKey(zone.change.Record.Desired.ID), Value: zone.value},
				Mutation{
					Type:  MutationPut,
					Key:   zoneNameKey(zone.change.Record.EnvironmentID, zone.change.Record.Desired.Name),
					Value: []byte(zone.change.Record.Desired.ID),
				},
				Mutation{
					Type:  MutationPut,
					Key:   zoneOwnerKey(zone.change.Record.EnvironmentID, zone.change.Record.Desired.ID),
					Value: []byte(zone.change.Record.Desired.ID),
				},
			)
		}
	}
	conditions = append(conditions, Condition{
		Key: zonePoolRegistryKey(environment.Record.ID), ModRevision: preparedZones.registry.Revision,
	})
	if len(preparedZones.registryValue) != 0 {
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: zonePoolRegistryKey(environment.Record.ID), Value: preparedZones.registryValue,
		})
	}
	for _, service := range preparedServices {
		primaryCondition := Condition{Key: serviceKey(service.change.Record.Desired.ID)}
		nameCondition := Condition{
			Key: serviceNameKey(service.change.Record.EnvironmentID, service.change.Record.Desired.Name),
		}
		ownerCondition := Condition{
			Key: serviceOwnerKey(service.change.Record.EnvironmentID, service.change.Record.Desired.ID),
		}
		if service.change.Current != nil {
			primaryCondition.ModRevision = service.change.Current.Revision
			nameCondition.ModRevision = service.nameRevision
			ownerCondition.ModRevision = service.ownerRevision
		}
		conditions = append(
			conditions,
			primaryCondition,
			nameCondition,
			ownerCondition,
			Condition{Key: deletionTombstoneKey("service", service.change.Record.Desired.ID)},
		)
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: serviceKey(service.change.Record.Desired.ID), Value: service.value,
		})
		if service.change.Current == nil {
			mutations = append(
				mutations,
				Mutation{
					Type:  MutationPut,
					Key:   serviceNameKey(service.change.Record.EnvironmentID, service.change.Record.Desired.Name),
					Value: []byte(service.change.Record.Desired.ID),
				},
				Mutation{
					Type:  MutationPut,
					Key:   serviceOwnerKey(service.change.Record.EnvironmentID, service.change.Record.Desired.ID),
					Value: []byte(service.change.Record.Desired.ID),
				},
			)
		}
	}
	for _, route := range preparedRoutes {
		primaryCondition := Condition{Key: routeKey(route.change.Record.Desired.ID)}
		ownerCondition := Condition{
			Key: routeOwnerKey(route.change.Record.EnvironmentID, route.change.Record.Desired.ID),
		}
		matchCondition := Condition{Key: routeMatchKey(
			route.change.Record.EnvironmentID,
			route.change.Record.Desired.Host,
			route.change.Record.Desired.Path,
		)}
		if route.change.Current != nil {
			primaryCondition.ModRevision = route.change.Current.Revision
			ownerCondition.ModRevision = route.ownerRevision
			matchCondition.ModRevision = route.matchRevision
		}
		conditions = append(
			conditions,
			primaryCondition,
			ownerCondition,
			matchCondition,
			Condition{Key: deletionTombstoneKey("route", route.change.Record.Desired.ID)},
		)
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: routeKey(route.change.Record.Desired.ID), Value: route.value,
		})
		if route.change.Current == nil {
			mutations = append(
				mutations,
				Mutation{
					Type:  MutationPut,
					Key:   routeOwnerKey(route.change.Record.EnvironmentID, route.change.Record.Desired.ID),
					Value: []byte(route.change.Record.Desired.ID),
				},
				Mutation{
					Type: MutationPut,
					Key: routeMatchKey(
						route.change.Record.EnvironmentID,
						route.change.Record.Desired.Host,
						route.change.Record.Desired.Path,
					),
					Value: []byte(route.change.Record.Desired.ID),
				},
			)
		}
	}
	componentPublication, err := prepareComponentTaskPublication(
		environment,
		task,
		zoneChanges,
		componentPreparation,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearPreparedComponentTaskPublication(componentPublication)
	conditions = append(conditions, componentPublication.conditions...)
	mutations = append(mutations, componentPublication.mutations...)
	baseClassifier := classifyEnvironmentBlueprintApplyConflict(
		len(revision.Files), expectedHeadRevision, project, environment, task.OperationID, preparedZones,
		preparedServices, preparedRoutes,
	)
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyEnvironmentBlueprintComponentPublication(baseClassifier, componentPublication),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyEnvironmentBlueprintApplyConflict(
	fileCount int,
	expectedHeadRevision int64,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	operationID string,
	zones preparedEnvironmentBlueprintZones,
	services []preparedEnvironmentBlueprintService,
	routes []preparedEnvironmentBlueprintRoute,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		baseCount := 12 + fileCount
		zoneRegistryIndex := baseCount + 4*len(zones.changes)
		if len(values) != zoneRegistryIndex+1+4*len(services)+4*len(routes) {
			return errs.New(errs.KindInternal, "Blueprint apply compare evidence is incomplete")
		}
		if err := classifyEnvironmentBlueprintBaseConflict(
			values[:baseCount], fileCount, expectedHeadRevision, project, environment, operationID,
		); err != nil {
			return err
		}
		for index, zone := range zones.changes {
			offset := baseCount + index*4
			primary := values[offset]
			name := values[offset+1]
			owner := values[offset+2]
			tombstone := values[offset+3]
			zoneID := zone.change.Record.Desired.ID
			if zone.change.Current == nil {
				if primary != nil || owner != nil {
					return errs.New(errs.KindStateConflict, "Zone stable identity is already in use")
				}
				if name != nil {
					return errs.New(errs.KindNameConflict, "Zone name is already in use")
				}
			} else {
				if primary == nil {
					return errs.New(errs.KindZoneNotFound, "Zone was not found")
				}
				if primary.ModRevision != zone.change.Current.Revision {
					return stateConflict("zone", zoneID)
				}
				if name == nil || owner == nil || string(name.Value) != zoneID || string(owner.Value) != zoneID {
					return errs.New(errs.KindInternal, "Zone indexes changed or are corrupt")
				}
			}
			if tombstone != nil {
				return errs.New(errs.KindResourceInUse, "Zone deletion is in progress")
			}
		}
		registry := values[zoneRegistryIndex]
		if (zones.registry.Revision == 0 && registry != nil) ||
			(zones.registry.Revision > 0 &&
				(registry == nil || registry.ModRevision != zones.registry.Revision)) {
			return stateConflict("Zone pool registry", environment.Record.ID)
		}
		serviceOffset := zoneRegistryIndex + 1
		for index, service := range services {
			offset := serviceOffset + index*4
			primary := values[offset]
			name := values[offset+1]
			owner := values[offset+2]
			tombstone := values[offset+3]
			serviceID := service.change.Record.Desired.ID
			if service.change.Current == nil {
				if primary != nil || owner != nil {
					return errs.New(errs.KindStateConflict, "Service stable identity is already in use")
				}
				if name != nil {
					return errs.New(errs.KindNameConflict, "Service name is already in use")
				}
			} else {
				if primary == nil {
					return errs.New(errs.KindServiceNotFound, "Service was not found")
				}
				if primary.ModRevision != service.change.Current.Revision {
					return stateConflict("service", serviceID)
				}
				if name == nil || owner == nil || string(name.Value) != serviceID || string(owner.Value) != serviceID {
					return errs.New(errs.KindInternal, "Service indexes changed or are corrupt")
				}
			}
			if tombstone != nil {
				return errs.New(errs.KindResourceInUse, "Service deletion is in progress")
			}
		}
		routeOffset := serviceOffset + 4*len(services)
		for index, route := range routes {
			offset := routeOffset + index*4
			primary := values[offset]
			owner := values[offset+1]
			match := values[offset+2]
			tombstone := values[offset+3]
			routeID := route.change.Record.Desired.ID
			if route.change.Current == nil {
				if primary != nil || owner != nil {
					return errs.New(errs.KindStateConflict, "Route stable identity is already in use")
				}
				if match != nil {
					return errs.New(errs.KindNameConflict, "Route host and path are already in use")
				}
			} else {
				if primary == nil {
					return errs.New(errs.KindRouteNotFound, "Route was not found")
				}
				if primary.ModRevision != route.change.Current.Revision {
					return stateConflict("route", routeID)
				}
				if owner == nil || match == nil || string(owner.Value) != routeID || string(match.Value) != routeID {
					return errs.New(errs.KindInternal, "Route indexes changed or are corrupt")
				}
			}
			if tombstone != nil {
				return errs.New(errs.KindResourceInUse, "Route deletion is in progress")
			}
		}
		return errs.New(errs.KindStateConflict, "Environment Blueprint resource state changed")
	}
}

func classifyEnvironmentBlueprintBaseConflict(
	values []*KeyValue,
	fileCount int,
	expectedHeadRevision int64,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	operationID string,
) error {
	if values[2] != nil {
		activeTaskID, err := decodeTaskReference(values[2].Value)
		if err != nil {
			return err
		}
		return errs.Newf(
			errs.KindStateConflict,
			"operation %s already has active task %s",
			operationID,
			activeTaskID,
		)
	}
	for _, index := range []int{0, 1, 3} {
		if values[index] != nil {
			return errs.New(errs.KindInternal, "Blueprint apply collided with durable Task state")
		}
	}
	for index := 4; index < 5+fileCount; index++ {
		if values[index] != nil {
			return errs.New(errs.KindInternal, "Blueprint apply collided with immutable revision state")
		}
	}
	headIndex := 5 + fileCount
	if (expectedHeadRevision == 0 && values[headIndex] != nil) ||
		(expectedHeadRevision > 0 && (values[headIndex] == nil || values[headIndex].ModRevision != expectedHeadRevision)) {
		return errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	projectionIndex := headIndex + 1
	if (expectedHeadRevision == 0 && values[projectionIndex] != nil) ||
		(expectedHeadRevision > 0 &&
			(values[projectionIndex] == nil || values[projectionIndex].ModRevision != expectedHeadRevision)) {
		return errs.New(errs.KindStateConflict, "Environment Compose projection changed")
	}
	environmentIndex := headIndex + 2
	projectIndex := headIndex + 3
	if values[environmentIndex] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	if values[environmentIndex].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[projectIndex] == nil {
		return errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if values[projectIndex].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	if values[headIndex+4] != nil {
		return errs.New(errs.KindResourceInUse, "Environment deletion is in progress")
	}
	if values[headIndex+5] != nil {
		return errs.New(errs.KindResourceInUse, "Project deletion is in progress")
	}
	if values[headIndex+6] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return nil
}

type preparedEnvironmentBlueprintService struct {
	change        EnvironmentBlueprintServiceChange
	value         []byte
	nameRevision  int64
	ownerRevision int64
}

type preparedEnvironmentBlueprintZone struct {
	change        EnvironmentBlueprintZoneChange
	value         []byte
	nameRevision  int64
	ownerRevision int64
}

type preparedEnvironmentBlueprintZones struct {
	changes       []preparedEnvironmentBlueprintZone
	registry      Versioned[zonePoolRegistry]
	registryValue []byte
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintZoneChanges(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	projection EnvironmentComposeProjection,
	changes []EnvironmentBlueprintZoneChange,
) (preparedEnvironmentBlueprintZones, error) {
	if len(changes) != len(projection.Networks) {
		return preparedEnvironmentBlueprintZones{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Zone changes do not cover the Compose network projection",
		)
	}
	identities := make(map[string]string, len(projection.Networks))
	for _, identity := range projection.Networks {
		identities[identity.ID] = identity.Name
	}
	zoneRepository, err := newZoneRepository(repository.store)
	if err != nil {
		return preparedEnvironmentBlueprintZones{}, err
	}
	registry, err := zoneRepository.getZonePoolRegistry(ctx, environment.Record.ID)
	if err != nil {
		return preparedEnvironmentBlueprintZones{}, err
	}
	result := preparedEnvironmentBlueprintZones{
		changes:  make([]preparedEnvironmentBlueprintZone, 0, len(changes)),
		registry: registry,
	}
	nextRegistry := registry.Record
	for _, change := range changes {
		if err := validateZoneRecord(change.Record); err != nil {
			clearPreparedEnvironmentBlueprintZones(result)
			return preparedEnvironmentBlueprintZones{}, err
		}
		zoneID := change.Record.Desired.ID
		if change.Record.EnvironmentID != environment.Record.ID ||
			identities[zoneID] != change.Record.Desired.Name {
			clearPreparedEnvironmentBlueprintZones(result)
			return preparedEnvironmentBlueprintZones{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Zone change does not match its network projection",
			)
		}
		item := preparedEnvironmentBlueprintZone{change: change}
		if change.Current != nil {
			if err := validateZoneRecord(change.Current.Record); err != nil || change.Current.Revision <= 0 ||
				change.Current.ReadRevision < change.Current.Revision || change.Current.Record != change.Record {
				clearPreparedEnvironmentBlueprintZones(result)
				return preparedEnvironmentBlueprintZones{}, errs.New(
					errs.KindValidationFailed,
					"Blueprint Zone replacement changed immutable desired state",
				)
			}
			if registry.Record.Reservations[zoneID] != change.Record.Desired.Subnet {
				clearPreparedEnvironmentBlueprintZones(result)
				return preparedEnvironmentBlueprintZones{}, errs.New(
					errs.KindInternal,
					"Zone subnet reservation is missing or corrupt",
				)
			}
			indexes, err := repository.store.GetMany(ctx, GetManyRequest{
				Keys: []string{
					zoneNameKey(environment.Record.ID, change.Record.Desired.Name),
					zoneOwnerKey(environment.Record.ID, zoneID),
				},
				Revision: change.Current.ReadRevision,
			})
			if err != nil {
				clearPreparedEnvironmentBlueprintZones(result)
				return preparedEnvironmentBlueprintZones{}, err
			}
			if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
				string(indexes.Values[0].Value) != zoneID || string(indexes.Values[1].Value) != zoneID {
				clearPreparedEnvironmentBlueprintZones(result)
				return preparedEnvironmentBlueprintZones{}, errs.New(
					errs.KindInternal,
					"Zone indexes are missing or corrupt",
				)
			}
			item.nameRevision = indexes.Values[0].ModRevision
			item.ownerRevision = indexes.Values[1].ModRevision
		} else {
			nextRegistry, err = nextRegistry.reserve(environment.Record, change.Record)
			if err != nil {
				clearPreparedEnvironmentBlueprintZones(result)
				return preparedEnvironmentBlueprintZones{}, err
			}
			value, err := encodeZoneRecord(change.Record)
			if err != nil {
				clearPreparedEnvironmentBlueprintZones(result)
				return preparedEnvironmentBlueprintZones{}, err
			}
			item.value = value
		}
		result.changes = append(result.changes, item)
	}
	if len(nextRegistry.Reservations) != len(registry.Record.Reservations) {
		result.registryValue, err = encodeEnvelope("zone_pool_registry", nextRegistry)
		if err != nil {
			clearPreparedEnvironmentBlueprintZones(result)
			return preparedEnvironmentBlueprintZones{}, err
		}
	}
	return result, nil
}

func clearPreparedEnvironmentBlueprintZones(zones preparedEnvironmentBlueprintZones) {
	for index := range zones.changes {
		clear(zones.changes[index].value)
	}
	clear(zones.registryValue)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintServiceChanges(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	projection EnvironmentComposeProjection,
	changes []EnvironmentBlueprintServiceChange,
) ([]preparedEnvironmentBlueprintService, error) {
	if len(changes) != len(projection.Services) {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint Service changes do not cover the Compose projection")
	}
	identities := make(map[string]string, len(projection.Services))
	for _, identity := range projection.Services {
		identities[identity.ID] = identity.Name
	}
	prepared := make([]preparedEnvironmentBlueprintService, 0, len(changes))
	for _, change := range changes {
		if err := validateServiceRecord(change.Record); err != nil {
			clearPreparedEnvironmentBlueprintServices(prepared)
			return nil, err
		}
		serviceID := change.Record.Desired.ID
		if change.Record.EnvironmentID != environment.Record.ID || change.Record.BackingNetworkID != "" ||
			identities[serviceID] != change.Record.Desired.Name {
			clearPreparedEnvironmentBlueprintServices(prepared)
			return nil, errs.New(errs.KindValidationFailed, "Blueprint Service change does not match its projection")
		}
		item := preparedEnvironmentBlueprintService{change: change}
		if change.Current != nil {
			if err := validateServiceVersion(*change.Current); err != nil {
				clearPreparedEnvironmentBlueprintServices(prepared)
				return nil, err
			}
			if change.Current.Record.EnvironmentID != environment.Record.ID ||
				change.Current.Record.Desired.ID != serviceID ||
				change.Current.Record.Desired.Name != change.Record.Desired.Name ||
				change.Current.Record.Runtime != change.Record.Runtime {
				clearPreparedEnvironmentBlueprintServices(prepared)
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Service replacement changed runtime or identity",
				)
			}
			indexes, err := repository.store.GetMany(ctx, GetManyRequest{
				Keys: []string{
					serviceNameKey(environment.Record.ID, change.Record.Desired.Name),
					serviceOwnerKey(environment.Record.ID, serviceID),
				},
				Revision: change.Current.ReadRevision,
			})
			if err != nil {
				clearPreparedEnvironmentBlueprintServices(prepared)
				return nil, err
			}
			if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
				string(indexes.Values[0].Value) != serviceID || string(indexes.Values[1].Value) != serviceID {
				clearPreparedEnvironmentBlueprintServices(prepared)
				return nil, errs.New(errs.KindInternal, "Service indexes are missing or corrupt")
			}
			item.nameRevision = indexes.Values[0].ModRevision
			item.ownerRevision = indexes.Values[1].ModRevision
		}
		value, err := encodeServiceRecord(change.Record)
		if err != nil {
			clearPreparedEnvironmentBlueprintServices(prepared)
			return nil, err
		}
		item.value = value
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func clearPreparedEnvironmentBlueprintServices(services []preparedEnvironmentBlueprintService) {
	for index := range services {
		clear(services[index].value)
	}
}

type preparedEnvironmentBlueprintRoute struct {
	change        EnvironmentBlueprintRouteChange
	value         []byte
	ownerRevision int64
	matchRevision int64
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintRouteChanges(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	services []EnvironmentBlueprintServiceChange,
	changes []EnvironmentBlueprintRouteChange,
) ([]preparedEnvironmentBlueprintRoute, error) {
	targets := make(map[string]struct{}, len(services))
	for _, service := range services {
		targets[service.Record.Desired.ID] = struct{}{}
	}
	seenIDs := make(map[string]struct{}, len(changes))
	seenMatches := make(map[string]struct{}, len(changes))
	prepared := make([]preparedEnvironmentBlueprintRoute, 0, len(changes))
	for _, change := range changes {
		if err := validateRouteRecord(change.Record); err != nil {
			clearPreparedEnvironmentBlueprintRoutes(prepared)
			return nil, err
		}
		routeID := change.Record.Desired.ID
		matchKey := routeMatchKey(
			change.Record.EnvironmentID,
			change.Record.Desired.Host,
			change.Record.Desired.Path,
		)
		_, targetExists := targets[change.Record.Desired.TargetServiceID]
		_, duplicateID := seenIDs[routeID]
		_, duplicateMatch := seenMatches[matchKey]
		if change.Record.EnvironmentID != environment.Record.ID || !targetExists || duplicateID || duplicateMatch {
			clearPreparedEnvironmentBlueprintRoutes(prepared)
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Route change does not match its Environment Service projection",
			)
		}
		seenIDs[routeID] = struct{}{}
		seenMatches[matchKey] = struct{}{}
		item := preparedEnvironmentBlueprintRoute{change: change}
		if change.Current != nil {
			if err := validateRouteVersion(*change.Current); err != nil {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, err
			}
			replacement, err := ReplaceRouteDesired(change.Current.Record, change.Record.Desired)
			if err != nil || replacement != change.Record {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Route replacement changed immutable identity, match, or target",
				)
			}
			indexes, err := repository.store.GetMany(ctx, GetManyRequest{
				Keys: []string{
					routeOwnerKey(environment.Record.ID, routeID),
					matchKey,
				},
				Revision: change.Current.ReadRevision,
			})
			if err != nil {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, err
			}
			if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
				string(indexes.Values[0].Value) != routeID || string(indexes.Values[1].Value) != routeID {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, errs.New(errs.KindInternal, "Route indexes are missing or corrupt")
			}
			item.ownerRevision = indexes.Values[0].ModRevision
			item.matchRevision = indexes.Values[1].ModRevision
		}
		value, err := encodeRouteRecord(change.Record)
		if err != nil {
			clearPreparedEnvironmentBlueprintRoutes(prepared)
			return nil, err
		}
		item.value = value
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func clearPreparedEnvironmentBlueprintRoutes(routes []preparedEnvironmentBlueprintRoute) {
	for index := range routes {
		clear(routes[index].value)
	}
}

func encodeEnvironmentBlueprintManifest(revision EnvironmentBlueprintRevision) ([]byte, error) {
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		return nil, err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID: revision.EnvironmentID, RevisionID: revision.RevisionID,
		RootPath: revision.RootPath, ComposeSources: append([]string(nil), revision.ComposeSources...),
		Interpolation: cloneEnvironmentBlueprintInterpolation(revision.Interpolation),
		Files:         make([]environmentBlueprintManifestFile, len(revision.Files)),
		CreatedAt:     revision.CreatedAt.Format(time.RFC3339Nano),
	}
	for index, file := range revision.Files {
		digest := sha256.Sum256(file.Content)
		manifest.Files[index] = environmentBlueprintManifestFile{
			Path: file.Path, Size: len(file.Content), SHA256: hex.EncodeToString(digest[:]),
		}
	}
	return encodeEnvelope("environment-blueprint-revision", manifest)
}

func decodeEnvironmentBlueprintManifest(value []byte) (environmentBlueprintManifest, error) {
	manifest, err := decodeEnvelope[environmentBlueprintManifest](value, "environment-blueprint-revision")
	if err != nil {
		return environmentBlueprintManifest{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
	if err != nil || manifest.CreatedAt != createdAt.UTC().Format(time.RFC3339Nano) {
		return environmentBlueprintManifest{}, corruptEnvironmentBlueprint()
	}
	manifest.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	if err := validateEnvironmentBlueprintManifest(manifest); err != nil {
		return environmentBlueprintManifest{}, corruptEnvironmentBlueprint()
	}
	return manifest, nil
}

func validateEnvironmentBlueprintRevision(revision EnvironmentBlueprintRevision) error {
	if err := validateID(ids.KindEnvironment, revision.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindTask, revision.RevisionID); err != nil {
		return err
	}
	if err := validateTimestamp("Blueprint revision created_at", revision.CreatedAt); err != nil {
		return err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID: revision.EnvironmentID, RevisionID: revision.RevisionID,
		RootPath: revision.RootPath, ComposeSources: revision.ComposeSources,
		Interpolation: revision.Interpolation, CreatedAt: revision.CreatedAt.Format(time.RFC3339Nano),
		Files: make([]environmentBlueprintManifestFile, len(revision.Files)),
	}
	for index, file := range revision.Files {
		digest := sha256.Sum256(file.Content)
		manifest.Files[index] = environmentBlueprintManifestFile{
			Path: file.Path, Size: len(file.Content), SHA256: hex.EncodeToString(digest[:]),
		}
	}
	return validateEnvironmentBlueprintManifest(manifest)
}

func validateEnvironmentBlueprintManifest(manifest environmentBlueprintManifest) error {
	if validateID(ids.KindEnvironment, manifest.EnvironmentID) != nil ||
		validateID(ids.KindTask, manifest.RevisionID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint revision identity is invalid")
	}
	if err := validateEnvironmentBlueprintPath(manifest.RootPath); err != nil {
		return err
	}
	if len(manifest.Files) == 0 || len(manifest.Files) > environmentBlueprintMaxFiles ||
		len(manifest.ComposeSources) == 0 || manifest.ComposeSources[0] != manifest.RootPath {
		return errs.New(errs.KindValidationFailed, "Blueprint revision file namespace is invalid")
	}
	declared := make(map[string]struct{}, len(manifest.Files))
	totalBytes := 0
	previous := ""
	for index, file := range manifest.Files {
		if validateEnvironmentBlueprintPath(file.Path) != nil || (index > 0 && file.Path <= previous) ||
			file.Size < 0 || file.Size > environmentBlueprintMaxFileBytes ||
			len(file.SHA256) != sha256.Size*2 {
			return errs.New(errs.KindValidationFailed, "Blueprint revision file metadata is invalid")
		}
		decoded, err := hex.DecodeString(file.SHA256)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != file.SHA256 {
			return errs.New(errs.KindValidationFailed, "Blueprint revision file digest is invalid")
		}
		totalBytes += file.Size
		if totalBytes > environmentBlueprintMaxTotalBytes {
			return errs.New(errs.KindValidationFailed, "Blueprint revision exceeds its total size limit")
		}
		declared[file.Path] = struct{}{}
		previous = file.Path
	}
	seenSources := make(map[string]struct{}, len(manifest.ComposeSources))
	for _, source := range manifest.ComposeSources {
		if validateEnvironmentBlueprintPath(source) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint revision Compose source is invalid")
		}
		if _, exists := declared[source]; !exists {
			return errs.New(errs.KindValidationFailed, "Blueprint revision Compose source is undeclared")
		}
		if _, duplicate := seenSources[source]; duplicate {
			return errs.New(errs.KindValidationFailed, "Blueprint revision Compose sources are duplicated")
		}
		seenSources[source] = struct{}{}
	}
	for key, value := range manifest.Interpolation {
		if !environmentBlueprintInterpolationKey.MatchString(key) || !utf8.ValidString(value) ||
			strings.ContainsRune(value, 0) {
			return errs.New(errs.KindValidationFailed, "Blueprint revision interpolation is invalid")
		}
	}
	return nil
}

func validateEnvironmentBlueprintPath(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) ||
		len(value) > environmentBlueprintMaxPathBytes || strings.Contains(value, `\`) || path.IsAbs(value) {
		return errs.New(errs.KindValidationFailed, "Blueprint revision path is invalid")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned != value || strings.HasPrefix(cleaned, "../") {
		return errs.New(errs.KindValidationFailed, "Blueprint revision path is invalid")
	}
	return nil
}

func cloneEnvironmentBlueprintInterpolation(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func corruptEnvironmentBlueprint() error {
	return errs.New(errs.KindInternal, "Environment Blueprint revision is corrupt")
}
