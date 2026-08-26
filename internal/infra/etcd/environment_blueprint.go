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

	EnvironmentDesiredRevisionParam = "desired_revision_id"
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
	return environmentBlueprintRevisionPrefix(
		environmentID,
		revisionID,
	) + fmt.Sprintf(
		"files/%06d",
		index+1,
	)
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
	rootResult, err := repository.store.Get(ctx, environmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if rootResult.Entry == nil {
		return Versioned[EnvironmentBlueprintRevision]{
			ReadRevision: rootResult.ReadRevision,
		}, false, nil
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootResult.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	stream, readRevision, err := repository.readEnvironmentBlueprintStream(ctx, seal, "audit")
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	defer clear(stream)
	revision, err := decodeEnvironmentBlueprintAuditStream(stream)
	if err != nil || revision.EnvironmentID != environmentID || revision.RevisionID != revisionID {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	return Versioned[EnvironmentBlueprintRevision]{
		Record: revision, Revision: rootResult.Entry.ModRevision,
		ReadRevision: readRevision,
	}, true, nil
}

// PublishEnvironmentDesiredRevisionWithTask atomically advances the sole
// Environment desired-state pointer and enqueues the Task pinned to that
// already sealed revision. Domain records are projections, never parallel
// desired-state authority in this transaction.
func (repository *HierarchyRepository) PublishEnvironmentDesiredRevisionWithTask(
	ctx context.Context,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection EnvironmentComposeProjection,
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
	if project.Record.Kind != ProjectKindTenant || project.Revision <= 0 ||
		environment.Revision <= 0 || project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID ||
		environment.Record.ProvisioningState != EnvironmentProvisioningReady ||
		expectedHeadRevision < 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict, "Environment is not ready for desired-state publication",
		)
	}
	if validateStableID(ids.KindEnvironment, revision.EnvironmentID) != nil ||
		validateStableID(ids.KindTask, revision.RevisionID) != nil ||
		revision.EnvironmentID != environment.Record.ID ||
		claim.EnvironmentID != revision.EnvironmentID || claim.RevisionID != revision.RevisionID ||
		claim.TaskID != task.ID || projection.EnvironmentID != revision.EnvironmentID ||
		projection.RevisionID != revision.RevisionID ||
		task.Params[EnvironmentDesiredRevisionParam] != revision.RevisionID ||
		task.Params[TaskMaterializationEnvironmentParam] != revision.EnvironmentID ||
		task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Environment desired revision publication identity is invalid",
		)
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Environment desired revision marker does not match its Task",
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
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}

	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, project, environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	previous, hasPrevious, err := repository.getEnvironmentBlueprintProjectionAtRevision(
		ctx, environment.Record.ID, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if (expectedHeadRevision == 0 && hasPrevious) ||
		(expectedHeadRevision > 0 && (!hasPrevious || previous.Revision != expectedHeadRevision)) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	if err := validateEnvironmentComposeProjectionPublicationAdvance(
		previous.Record, hasPrevious, projection, task,
	); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	publication, err := repository.prepareEnvironmentBlueprintPublication(
		ctx, claim, revision, projection, task, marker, expectedHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)

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
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)

	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: environmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID), ModRevision: publication.rootRevision},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: environmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: MutationDelete, Key: publication.locatorKey},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(revision.EnvironmentID), Value: reference},
		epochMutation,
	}
	baseCount := 8
	classifier := func(_ int64, values []*KeyValue) error {
		if len(values) != baseCount+len(fence.conditions) {
			return errs.New(errs.KindInternal, "Environment desired publication compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, decodeErr := decodeTaskReference(values[2].Value)
			if decodeErr != nil {
				return decodeErr
			}
			return errs.Newf(errs.KindStateConflict, "operation %s already has active task %s", task.OperationID, activeTaskID)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Environment desired publication collided with durable Task state")
			}
		}
		for _, index := range []int{4, 5, 6} {
			if values[index] == nil {
				return errs.New(errs.KindStateConflict, "Environment sealed staging evidence changed")
			}
		}
		head := values[7]
		if (expectedHeadRevision == 0 && head != nil) ||
			(expectedHeadRevision > 0 && (head == nil || head.ModRevision != expectedHeadRevision)) {
			return errs.New(errs.KindStateConflict, "Environment desired state changed")
		}
		if conflict := fence.classifyCAS(values[baseCount:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "Environment desired publication raced")
	}
	taskTenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx, repository.store, project, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironmentDesiredPublicationBudget(plan, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateEnvironmentDesiredPublicationBudget(
	plan *idempotencyMutationPlan,
	marker IdempotencyMarker,
) error {
	if plan == nil {
		return errs.New(errs.KindInternal, "Environment desired publication plan is absent")
	}
	comparisons := len(plan.conditions) + 1
	mutations := len(plan.mutations) + 1
	if marker.ReplayTarget != nil {
		comparisons++
		mutations++
	}
	if !marker.RetainUntil.IsZero() {
		mutations++
	}
	if comparisons > 32 || mutations > 32 {
		return errs.New(
			errs.KindValidationFailed,
			"Environment desired publication exceeds the 32-comparison or 32-mutation ceiling",
		)
	}
	return nil
}

func classifyEnvironmentBlueprintApplyConflict(
	fileCount int,
	expectedHeadRevision int64,
	environment Versioned[EnvironmentRecord],
	operationID string,
	zones preparedEnvironmentBlueprintZones,
	services []preparedEnvironmentBlueprintService,
	routes []preparedEnvironmentBlueprintRoute,
	fence environmentMutationFenceEvidence,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		baseCount := 7 + fileCount
		zoneRegistryIndex := baseCount + 4*len(zones.changes)
		domainCount := zoneRegistryIndex + 1 + 4*len(services) + 4*len(routes)
		if len(values) != domainCount+len(fence.conditions) {
			return errs.New(errs.KindInternal, "Blueprint apply compare evidence is incomplete")
		}
		if err := classifyEnvironmentBlueprintBaseConflict(
			values[:baseCount], fileCount, expectedHeadRevision, operationID,
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
					return errs.New(
						errs.KindStateConflict,
						"Zone stable identity is already in use",
					)
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
				if name == nil || owner == nil || string(name.Value) != zoneID ||
					string(owner.Value) != zoneID {
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
					return errs.New(
						errs.KindStateConflict,
						"Service stable identity is already in use",
					)
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
				if name == nil || owner == nil || string(name.Value) != serviceID ||
					string(owner.Value) != serviceID {
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
					return errs.New(
						errs.KindStateConflict,
						"Route stable identity is already in use",
					)
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
				if owner == nil || match == nil || string(owner.Value) != routeID ||
					string(match.Value) != routeID {
					return errs.New(errs.KindInternal, "Route indexes changed or are corrupt")
				}
			}
			if tombstone != nil {
				return errs.New(errs.KindResourceInUse, "Route deletion is in progress")
			}
		}
		if conflict := fence.classifyCAS(values[domainCount:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "Environment Blueprint resource state changed")
	}
}

func classifyEnvironmentBlueprintBaseConflict(
	values []*KeyValue,
	fileCount int,
	expectedHeadRevision int64,
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
			return errs.New(
				errs.KindInternal,
				"Blueprint apply collided with immutable revision state",
			)
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
	return nil
}

func (repository *HierarchyRepository) loadEnvironmentBlueprintMutationFence(
	ctx context.Context,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
) (environmentMutationFenceEvidence, error) {
	keys := []string{environmentKey(environment.Record.ID), projectKey(project.Record.ID)}
	anchor, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return environmentMutationFenceEvidence{}, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 || len(anchor.Values) != len(keys) ||
		anchor.Values[0] == nil || anchor.Values[1] == nil {
		return environmentMutationFenceEvidence{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint hierarchy is unavailable",
		)
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0].ModRevision != environment.Revision ||
		anchor.Values[1].ModRevision != project.Revision {
		return environmentMutationFenceEvidence{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint hierarchy changed",
		)
	}
	return loadOrdinaryEnvironmentMutationFence(
		ctx, repository.store, environment.Record.ID, anchor.ReadRevision,
	)
}

func (repository *HierarchyRepository) getEnvironmentBlueprintProjectionAtRevision(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (Versioned[EnvironmentComposeProjection], bool, error) {
	return repository.getEnvironmentComposeProjectionAtRevision(ctx, environmentID, readRevision)
}

func environmentBlueprintTransactionOperationCount(
	plan *idempotencyMutationPlan,
	marker IdempotencyMarker,
) int {
	if plan == nil {
		return maximumTransactionOperations + 1
	}
	conditions := len(plan.conditions) + 1
	mutations := len(plan.mutations) + 1
	if marker.ReplayTarget != nil {
		conditions++
		mutations++
	}
	if !marker.RetainUntil.IsZero() {
		mutations++
	}
	return conditions + mutations
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
	return repository.prepareEnvironmentBlueprintZoneChangesAtRevision(
		ctx,
		environment,
		projection,
		changes,
		0,
	)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintZoneChangesAtRevision(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	projection EnvironmentComposeProjection,
	changes []EnvironmentBlueprintZoneChange,
	readRevision int64,
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
	registry, err := repository.getEnvironmentBlueprintZoneRegistryAtRevision(
		ctx, zoneRepository, environment.Record.ID, readRevision,
	)
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
			if err := validateZoneRecord(
				change.Current.Record,
			); err != nil || change.Current.Revision <= 0 ||
				change.Current.ReadRevision < change.Current.Revision ||
				change.Current.Record != change.Record {
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
				Revision: readRevision,
			})
			if err != nil {
				clearPreparedEnvironmentBlueprintZones(result)
				return preparedEnvironmentBlueprintZones{}, err
			}
			if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil ||
				indexes.Values[1] == nil ||
				string(indexes.Values[0].Value) != zoneID ||
				string(indexes.Values[1].Value) != zoneID {
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

func (repository *HierarchyRepository) getEnvironmentBlueprintZoneRegistryAtRevision(
	ctx context.Context,
	zones *ZoneRepository,
	environmentID string,
	readRevision int64,
) (Versioned[zonePoolRegistry], error) {
	if readRevision == 0 {
		return zones.getZonePoolRegistry(ctx, environmentID)
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{zonePoolRegistryKey(environmentID)}, Revision: readRevision,
	})
	if err != nil {
		return Versioned[zonePoolRegistry]{}, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 {
		return Versioned[zonePoolRegistry]{}, errs.New(
			errs.KindInternal,
			"Zone pool registry read is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	if result.Values[0] == nil {
		return Versioned[zonePoolRegistry]{
			Record: zonePoolRegistry{Reservations: map[string]string{}}, ReadRevision: readRevision,
		}, nil
	}
	registry, err := decodeEnvelope[zonePoolRegistry](result.Values[0].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(registry) != nil {
		return Versioned[zonePoolRegistry]{}, corruptZonePoolRegistry()
	}
	return Versioned[zonePoolRegistry]{
		Record: registry, Revision: result.Values[0].ModRevision, ReadRevision: readRevision,
	}, nil
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
	return repository.prepareEnvironmentBlueprintServiceChangesAtRevision(
		ctx,
		environment,
		projection,
		changes,
		0,
	)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintServiceChangesAtRevision(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	projection EnvironmentComposeProjection,
	changes []EnvironmentBlueprintServiceChange,
	readRevision int64,
) ([]preparedEnvironmentBlueprintService, error) {
	if len(changes) != len(projection.Services) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint Service changes do not cover the Compose projection",
		)
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
		if change.Record.EnvironmentID != environment.Record.ID ||
			change.Record.BackingNetworkID != "" ||
			identities[serviceID] != change.Record.Desired.Name {
			clearPreparedEnvironmentBlueprintServices(prepared)
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Service change does not match its projection",
			)
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
				Revision: readRevision,
			})
			if err != nil {
				clearPreparedEnvironmentBlueprintServices(prepared)
				return nil, err
			}
			if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil ||
				indexes.Values[1] == nil ||
				string(indexes.Values[0].Value) != serviceID ||
				string(indexes.Values[1].Value) != serviceID {
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
	return repository.prepareEnvironmentBlueprintRouteChangesAtRevision(
		ctx,
		environment,
		services,
		changes,
		0,
	)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintRouteChangesAtRevision(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	services []EnvironmentBlueprintServiceChange,
	changes []EnvironmentBlueprintRouteChange,
	readRevision int64,
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
		if change.Record.EnvironmentID != environment.Record.ID || !targetExists || duplicateID ||
			duplicateMatch {
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
				Revision: readRevision,
			})
			if err != nil {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, err
			}
			if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil ||
				indexes.Values[1] == nil ||
				string(indexes.Values[0].Value) != routeID ||
				string(indexes.Values[1].Value) != routeID {
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
		EnvironmentID:  revision.EnvironmentID,
		RevisionID:     revision.RevisionID,
		RootPath:       revision.RootPath,
		ComposeSources: append([]string(nil), revision.ComposeSources...),
		Interpolation:  cloneEnvironmentBlueprintInterpolation(revision.Interpolation),
		Files:          make([]environmentBlueprintManifestFile, len(revision.Files)),
		CreatedAt:      revision.CreatedAt.Format(time.RFC3339Nano),
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
	manifest, err := decodeEnvelope[environmentBlueprintManifest](
		value,
		"environment-blueprint-revision",
	)
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
		EnvironmentID:  revision.EnvironmentID,
		RevisionID:     revision.RevisionID,
		RootPath:       revision.RootPath,
		ComposeSources: revision.ComposeSources,
		Interpolation:  revision.Interpolation,
		CreatedAt:      revision.CreatedAt.Format(time.RFC3339Nano),
		Files:          make([]environmentBlueprintManifestFile, len(revision.Files)),
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
		if validateEnvironmentBlueprintPath(file.Path) != nil ||
			(index > 0 && file.Path <= previous) ||
			file.Size < 0 ||
			file.Size > environmentBlueprintMaxFileBytes ||
			len(file.SHA256) != sha256.Size*2 {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision file metadata is invalid",
			)
		}
		decoded, err := hex.DecodeString(file.SHA256)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != file.SHA256 {
			return errs.New(errs.KindValidationFailed, "Blueprint revision file digest is invalid")
		}
		totalBytes += file.Size
		if totalBytes > environmentBlueprintMaxTotalBytes {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision exceeds its total size limit",
			)
		}
		declared[file.Path] = struct{}{}
		previous = file.Path
	}
	seenSources := make(map[string]struct{}, len(manifest.ComposeSources))
	for _, source := range manifest.ComposeSources {
		if validateEnvironmentBlueprintPath(source) != nil {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose source is invalid",
			)
		}
		if _, exists := declared[source]; !exists {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose source is undeclared",
			)
		}
		if _, duplicate := seenSources[source]; duplicate {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose sources are duplicated",
			)
		}
		seenSources[source] = struct{}{}
	}
	for key, value := range manifest.Interpolation {
		if !environmentBlueprintInterpolationKey.MatchString(key) || !utf8.ValidString(value) ||
			strings.ContainsRune(value, 0) {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision interpolation is invalid",
			)
		}
	}
	return nil
}

func validateEnvironmentBlueprintPath(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) ||
		len(
			value,
		) > environmentBlueprintMaxPathBytes || strings.Contains(value, `\`) || path.IsAbs(value) {
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
