package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/netip"
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
	if seal.SourceKind == EnvironmentBlueprintSourceMutation {
		return Versioned[EnvironmentBlueprintRevision]{ReadRevision: rootResult.ReadRevision}, false, nil
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

type preparedEnvironmentBlueprintPoolChange struct {
	environment      Versioned[EnvironmentRecord]
	registryRevision int64
	environmentValue []byte
	registryValue    []byte
}

func (change preparedEnvironmentBlueprintPoolChange) changed() bool {
	return len(change.environmentValue) != 0
}
func clearPreparedEnvironmentBlueprintPoolChange(change preparedEnvironmentBlueprintPoolChange) {
	clear(change.environmentValue)
	clear(change.registryValue)
}
func (repository *HierarchyRepository) prepareEnvironmentBlueprintPoolChangeAtRevision(
	ctx context.Context,
	root netip.Prefix,
	current Versioned[EnvironmentRecord],
	desiredNetworkPool string,
	revision int64,
) (preparedEnvironmentBlueprintPoolChange, error) {
	prepared := preparedEnvironmentBlueprintPoolChange{environment: current}
	if desiredNetworkPool == current.Record.NetworkPool {
		return prepared, nil
	}
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool root must be a canonical IPv4 CIDR",
		)
	}
	prepared.environment.Record.NetworkPool = desiredNetworkPool
	if err := validateEnvironment(prepared.environment.Record); err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	registries, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{environmentPoolRegistryKey}, Revision: revision,
	})
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	if registries == nil || len(registries.Values) != 1 || registries.Values[0] == nil ||
		registries.Values[0].Key != environmentPoolRegistryKey {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindInternal,
			"Environment pool reservation registry is missing",
		)
	}
	defer clearKeyValues(registries.Values)
	global, err := decodeEnvelope[EnvironmentPoolRegistry](
		registries.Values[0].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(global) != nil {
		return preparedEnvironmentBlueprintPoolChange{}, corruptEnvironmentPoolRegistry()
	}
	nextGlobal, canonical, err := global.Replace(
		root,
		current.Record.ID,
		current.Record.NetworkPool,
		desiredNetworkPool,
	)
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	if canonical != desiredNetworkPool {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindValidationFailed,
			"x-gp-network-pool must be a canonical IPv4 CIDR",
		)
	}
	prepared.environmentValue, err = encodeEnvironment(prepared.environment.Record)
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	prepared.registryValue, err = encodeEnvelope("environment_pool_registry", nextGlobal)
	if err != nil {
		clear(prepared.environmentValue)
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	prepared.registryRevision = registries.Values[0].ModRevision
	return prepared, nil
}

// PublishEnvironmentDesiredRevisionWithTask atomically advances the sole
// Environment desired-state pointer and enqueues the Task pinned to that
// already sealed revision. Direct desired mutations preserve the existing
// Environment pool.
func (repository *HierarchyRepository) PublishEnvironmentDesiredRevisionWithTask(
	ctx context.Context,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection EnvironmentComposeProjection,
	zoneChanges []EnvironmentBlueprintZoneChange,
	serviceChanges []EnvironmentBlueprintServiceChange,
	routeChanges []EnvironmentBlueprintRouteChange,
	releaseGroupPreparation ReleaseGroupBlueprintPreparedMutation,
	componentPreparation ComponentTaskPreparation,
	attachPreparation BlueprintAttachTaskPreparation,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if claim.SourceKind != EnvironmentBlueprintSourceMutation {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"direct Environment desired publication requires mutation source authority",
		)
	}
	return repository.publishEnvironmentDesiredRevisionWithTask(
		ctx, netip.Prefix{}, environment.Record.NetworkPool,
		project, environment, expectedHeadRevision, claim, revision, projection,
		zoneChanges, serviceChanges, routeChanges, releaseGroupPreparation,
		componentPreparation, attachPreparation, BlueprintBackupPolicyPreparation{},
		BlueprintScriptPublication{}, BlueprintReleasePublication{},
		BlueprintRequirementGate{}, task, marker, nil,
	)
}

// PublishEnvironmentBlueprintDesiredRevision publishes the one authored
// Blueprint Task with its prepared Script and candidate Release fragments.
func (repository *EnvironmentBlueprintRepository) PublishEnvironmentBlueprintDesiredRevision(
	ctx context.Context,
	environmentPool netip.Prefix,
	desiredNetworkPool string,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection EnvironmentComposeProjection,
	zoneChanges []EnvironmentBlueprintZoneChange,
	serviceChanges []EnvironmentBlueprintServiceChange,
	routeChanges []EnvironmentBlueprintRouteChange,
	releaseGroupPreparation ReleaseGroupBlueprintPreparedMutation,
	componentPreparation ComponentTaskPreparation,
	attachPreparation BlueprintAttachTaskPreparation,
	backupPreparation BlueprintBackupPolicyPreparation,
	scriptPublication BlueprintScriptPublication,
	releasePublication BlueprintReleasePublication,
	requirementGate BlueprintRequirementGate,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if claim.SourceKind != EnvironmentBlueprintSourceApply {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment Blueprint publication requires apply source authority",
		)
	}
	return repository.publishEnvironmentDesiredRevisionWithTask(
		ctx, environmentPool, desiredNetworkPool,
		project, environment, expectedHeadRevision, claim, revision, projection,
		zoneChanges, serviceChanges, routeChanges, releaseGroupPreparation,
		componentPreparation, attachPreparation, backupPreparation, scriptPublication, releasePublication,
		requirementGate, task, marker, repository.transactions,
	)
}

func (repository *HierarchyRepository) publishEnvironmentDesiredRevisionWithTask(
	ctx context.Context,
	environmentPool netip.Prefix,
	desiredNetworkPool string,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection EnvironmentComposeProjection,
	zoneChanges []EnvironmentBlueprintZoneChange,
	serviceChanges []EnvironmentBlueprintServiceChange,
	routeChanges []EnvironmentBlueprintRouteChange,
	releaseGroupPreparation ReleaseGroupBlueprintPreparedMutation,
	componentPreparation ComponentTaskPreparation,
	attachPreparation BlueprintAttachTaskPreparation,
	backupPreparation BlueprintBackupPolicyPreparation,
	scriptPublication BlueprintScriptPublication,
	releasePublication BlueprintReleasePublication,
	requirementGate BlueprintRequirementGate,
	task TaskRecord,
	marker IdempotencyMarker,
	blueprintTransactions environmentBlueprintTransactionStore,
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
	if err := validateReleaseGroupBlueprintPreparedMutation(releaseGroupPreparation, environment.Record.ID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := scriptPublication.validate(environment.Record.ID, claim.SourceKind); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := releasePublication.validate(environment.Record.ID, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := releasePublication.validateRetainedRuntime(task, projection); err != nil {
		return IdempotencyTransactionResult{}, err
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
	poolChange, err := repository.prepareEnvironmentBlueprintPoolChangeAtRevision(
		ctx,
		environmentPool,
		environment,
		desiredNetworkPool,
		fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearPreparedEnvironmentBlueprintPoolChange(poolChange)
	effectiveEnvironment := poolChange.environment
	scriptRemoval, err := repository.prepareDesiredScriptRemoval(
		ctx, environment.Record.ID, expectedHeadRevision, fence.readAtRevision(), projection, task,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	zonePool, err := repository.prepareEnvironmentBlueprintZonePoolAtRevision(
		ctx, effectiveEnvironment.Record, projection.DesiredZones, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(zonePool.value)
	publishDomain := claim.SourceKind == EnvironmentBlueprintSourceApply
	requirementPublication, err := prepareBlueprintRequirementGatePublication(
		requirementGate,
		task,
		projection,
		publishDomain && len(projection.BlueprintRequirements.Resolved) != 0,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer requirementPublication.clear()
	var componentPublication preparedComponentTaskPublication
	var attachPublication preparedBlueprintAttachTaskPublication
	var backupPublication preparedBlueprintBackupPolicyPublication
	if publishDomain {
		componentPublication, err = repository.prepareComponentTaskPublication(
			ctx, effectiveEnvironment, task, zoneChanges, componentPreparation,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearPreparedComponentTaskPublication(componentPublication)
		attachPublication, err = prepareBlueprintAttachTaskPublication(
			effectiveEnvironment, projection, task, attachPreparation,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearPreparedBlueprintAttachTaskPublication(attachPublication)
		backupPublication, err = prepareBlueprintBackupPolicyPublication(
			task, projection, attachPreparation, backupPreparation,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearPreparedBlueprintBackupPolicyPublication(backupPublication)
	} else if len(zoneChanges) != 0 || len(serviceChanges) != 0 || len(routeChanges) != 0 ||
		!releaseGroupPreparation.isZero() ||
		!componentTaskPreparationIsZero(componentPreparation) ||
		!blueprintAttachTaskPreparationIsZero(attachPreparation) ||
		!backupPreparation.IsZero() ||
		!scriptPublication.IsZero() || !releasePublication.IsZero() {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment desired mutation cannot publish Blueprint domain changes",
		)
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
		{
			Key:         environmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: environmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
		{Key: zonePoolRegistryKey(revision.EnvironmentID), ModRevision: zonePool.currentRevision},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: MutationDelete, Key: publication.locatorKey},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(revision.EnvironmentID), Value: reference},
		{Type: MutationPut, Key: zonePoolRegistryKey(revision.EnvironmentID), Value: zonePool.value},
	}
	zonePoolConditionIndex := len(conditions) - 1
	poolRegistryConditionIndex := -1
	if poolChange.changed() {
		poolRegistryConditionIndex = len(conditions)
		conditions = append(conditions, Condition{
			Key: environmentPoolRegistryKey, ModRevision: poolChange.registryRevision,
		})
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: environmentKey(environment.Record.ID), Value: poolChange.environmentValue},
			Mutation{Type: MutationPut, Key: environmentPoolRegistryKey, Value: poolChange.registryValue},
		)
	}
	baseCount := len(conditions)
	baseClassifier := func(_ int64, values []*KeyValue) error {
		if len(values) != baseCount+len(fence.conditions) {
			return errs.New(errs.KindInternal, "Environment desired publication compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, decodeErr := decodeTaskReference(values[2].Value)
			if decodeErr != nil {
				return decodeErr
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				task.OperationID,
				activeTaskID,
			)
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
		zoneRegistry := values[zonePoolConditionIndex]
		if (zonePool.currentRevision == 0 && zoneRegistry != nil) ||
			(zonePool.currentRevision > 0 &&
				(zoneRegistry == nil || zoneRegistry.ModRevision != zonePool.currentRevision)) {
			return stateConflict("Zone pool registry", environment.Record.ID)
		}
		if poolRegistryConditionIndex >= 0 {
			registry := values[poolRegistryConditionIndex]
			if registry == nil || registry.ModRevision != poolChange.registryRevision {
				return stateConflict("environment pool registry", environment.Record.ID)
			}
		}
		if conflict := fence.classifyCAS(values[baseCount:]); conflict != nil {
			return conflict
		}
		return nil
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations = append(mutations, epochMutation)
	classified := scriptRemoval.classifyConflict(len(conditions), baseClassifier)
	conditions = append(conditions, scriptRemoval.conditions...)
	requirementBaseConditionCount := len(conditions)
	conditions = append(conditions, requirementPublication.conditions...)
	mutations = append(mutations, requirementPublication.mutations...)
	requirementBaseClassifier := classified
	classified = func(revision int64, values []*KeyValue) error {
		if len(values) != requirementBaseConditionCount+len(requirementPublication.conditions) {
			return errs.New(errs.KindInternal, "Blueprint requirement publication compare evidence is incomplete")
		}
		if err := requirementBaseClassifier(revision, values[:requirementBaseConditionCount]); err != nil {
			return err
		}
		return requirementPublication.classify(values[requirementBaseConditionCount:])
	}
	if publishDomain {
		conditions, classified, err = composeEnvironmentBlueprintComponentPublication(
			conditions,
			classified,
			componentPublication,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		mutations = append(mutations, componentPublication.mutations...)
		conditions = append(conditions, attachPublication.conditions...)
		mutations = append(mutations, attachPublication.mutations...)
		classified = classifyEnvironmentBlueprintAttachPublication(classified, attachPublication)
		conditions = append(conditions, backupPublication.conditions...)
		mutations = append(mutations, backupPublication.mutations...)
		classified = classifyEnvironmentBlueprintBackupPolicyPublication(classified, backupPublication)
		releaseGroupBaseConditionCount := len(conditions)
		conditions = append(conditions, releaseGroupPreparation.conditions...)
		mutations = append(mutations, releaseGroupPreparation.mutations...)
		previousClassifier := classified
		classified = func(revision int64, values []*KeyValue) error {
			if len(values) != releaseGroupBaseConditionCount+len(releaseGroupPreparation.conditions) {
				return errs.New(errs.KindInternal, "Blueprint Release Group compare evidence is incomplete")
			}
			return previousClassifier(revision, values[:releaseGroupBaseConditionCount])
		}
		scriptBaseConditionCount := len(conditions)
		conditions = append(conditions, scriptPublication.conditions...)
		mutations = append(mutations, scriptPublication.mutations...)
		scriptBaseClassifier := classified
		classified = func(revision int64, values []*KeyValue) error {
			if len(values) != scriptBaseConditionCount+len(scriptPublication.conditions) {
				return errs.New(errs.KindInternal, "Blueprint Script compare evidence is incomplete")
			}
			if err := scriptBaseClassifier(revision, values[:scriptBaseConditionCount]); err != nil {
				return err
			}
			return scriptPublication.classify(values[scriptBaseConditionCount:])
		}
		releaseForCompare, err := releasePublication.withExistingComparisons(conditions)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		releaseBaseConditionCount := len(conditions)
		conditions = append(conditions, releaseForCompare.conditions...)
		mutations = append(mutations, releasePublication.mutations...)
		if err := releasePublication.sources.ValidateStagedMutations(claim, mutations); err != nil {
			return IdempotencyTransactionResult{}, err
		}
		releaseBaseClassifier := classified
		classified = func(revision int64, values []*KeyValue) error {
			if len(values) != releaseBaseConditionCount+len(releaseForCompare.conditions) {
				return errs.New(errs.KindInternal, "Blueprint Release compare evidence is incomplete")
			}
			if err := releaseBaseClassifier(revision, values[:releaseBaseConditionCount]); err != nil {
				return err
			}
			return releaseForCompare.classify(values[releaseBaseConditionCount:])
		}
	}
	classifier := func(revision int64, values []*KeyValue) error {
		if conflict := classified(revision, values); conflict != nil {
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
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, effectiveEnvironment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if !publishDomain {
		if err := plan.enforceTransactionBounds(validateEnvironmentDesiredPublicationBudget); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if publishDomain {
		return idempotency.applyEnvironmentBlueprint(ctx, marker, plan, blueprintTransactions)
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateEnvironmentDesiredPublicationBudget(
	conditions []Condition,
	mutations []Mutation,
) error {
	return validateEnvironmentDesiredPublicationPartitionCounts(
		len(conditions),
		len(mutations),
		len(conditions),
	)
}

func validateEnvironmentDesiredPublicationPartitionCounts(
	comparisons int,
	successMutations int,
	failureReads int,
) error {
	if comparisons > 32 || successMutations > 32 || failureReads > 32 {
		return errs.Newf(
			errs.KindValidationFailed,
			"Environment desired publication exceeds a 32-operation transaction partition (%d/%d/%d)",
			comparisons,
			successMutations,
			failureReads,
		)
	}
	return nil
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

type preparedEnvironmentBlueprintZonePool struct {
	currentRevision int64
	value           []byte
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintZonePoolAtRevision(
	ctx context.Context,
	environment EnvironmentRecord,
	desired []EnvironmentZoneProjection,
	readRevision int64,
) (preparedEnvironmentBlueprintZonePool, error) {
	current, err := repository.getEnvironmentBlueprintZoneRegistryAtRevision(
		ctx, environment.ID, readRevision,
	)
	if err != nil {
		return preparedEnvironmentBlueprintZonePool{}, err
	}
	next := zonePoolRegistry{Reservations: make(map[string]string, len(desired))}
	for _, projection := range desired {
		zone := ZoneRecord{EnvironmentID: projection.EnvironmentID, Desired: projection.Desired}
		if projection.EnvironmentID != environment.ID {
			return preparedEnvironmentBlueprintZonePool{}, errs.New(
				errs.KindValidationFailed, "Blueprint Zone does not belong to its Environment",
			)
		}
		next, err = next.reserve(environment, zone)
		if err != nil {
			return preparedEnvironmentBlueprintZonePool{}, err
		}
	}
	value, err := encodeEnvelope("zone_pool_registry", next)
	if err != nil {
		return preparedEnvironmentBlueprintZonePool{}, err
	}
	return preparedEnvironmentBlueprintZonePool{currentRevision: current.Revision, value: value}, nil
}

func (repository *HierarchyRepository) getEnvironmentBlueprintZoneRegistryAtRevision(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (Versioned[zonePoolRegistry], error) {
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
		}
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
