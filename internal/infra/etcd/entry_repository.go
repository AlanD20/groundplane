package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryOwnerPrefix = "/v1/indexes/entries/by-owner/environment/"

const blueprintEntryEnvironmentPrefix = "/v1/indexes/entries/blueprint-environment/"

// EntryValueGeneration is the closed atomic value input for an Entry mutation.
type EntryValueGeneration struct {
	Plain  *PlainEntryValueGeneration
	Secret *SecretEntryValueGeneration
}

type EntryRepository struct {
	store hierarchyStore
}

// BindBlueprintEntryEnvironment installs a derived lookup route. The current
// Environment projection remains the authority, so an index written before
// head publication cannot make staged Entry state visible.
func (repository *EntryRepository) BindBlueprintEntryEnvironment(
	ctx context.Context,
	environmentID string,
	entryID string,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if validateStableID(ids.KindEnvironment, environmentID) != nil || validateStableID(ids.KindEnvEntry, entryID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint Entry lookup identity is invalid")
	}
	key := blueprintEntryEnvironmentPrefix + entryID
	result, err := repository.store.Transact(ctx, []Condition{{Key: key}}, []Mutation{{Type: MutationPut, Key: key, Value: []byte(environmentID)}})
	if err != nil {
		return err
	}
	if result.Succeeded {
		return nil
	}
	existing, err := repository.store.Get(ctx, key)
	if err != nil {
		return err
	}
	if existing != nil && existing.Entry != nil && string(existing.Entry.Value) == environmentID {
		return nil
	}
	return errs.New(errs.KindStateConflict, "Blueprint Entry lookup identity is already occupied")
}

func (repository *EntryRepository) ResolveBlueprintEntryEnvironment(
	ctx context.Context,
	entryID string,
) (string, bool, error) {
	if err := validateContext(ctx); err != nil {
		return "", false, err
	}
	if validateStableID(ids.KindEnvEntry, entryID) != nil {
		return "", false, errs.New(errs.KindValidationFailed, "Blueprint Entry lookup id is invalid")
	}
	result, err := repository.store.Get(ctx, blueprintEntryEnvironmentPrefix+entryID)
	if err != nil {
		return "", false, err
	}
	if result == nil {
		return "", false, errs.New(errs.KindInternal, "Blueprint Entry lookup read is empty")
	}
	if result.Entry == nil {
		return "", false, nil
	}
	environmentID := string(result.Entry.Value)
	if validateStableID(ids.KindEnvironment, environmentID) != nil {
		return "", false, errs.New(errs.KindInternal, "Blueprint Entry lookup is corrupt")
	}
	return environmentID, true, nil
}

func NewEntryRepository(store Store) (*EntryRepository, error) {
	return newEntryRepository(store)
}

func newEntryRepository(store hierarchyStore) (*EntryRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Entry store is required")
	}
	return &EntryRepository{store: store}, nil
}

func (repository *EntryRepository) CreateEntry(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record EntryRecord,
	generation EntryValueGeneration,
) (Versioned[EntryRecord], error) {
	if err := validateEntryHierarchy(ctx, environment, project, record); err != nil {
		return Versioned[EntryRecord]{}, err
	}
	primaryValue, err := encodeEntryRecord(record)
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(record, generation)
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	defer clear(generationValue)
	fence, _, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryRecordKey(record.Entry.ID),
			entryOwnerKey(record.EnvironmentID, record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(DeletionTargetEntry), record.Entry.ID),
		},
		-1,
		record.Entry.ID,
	)
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(
		entryWriteConditions(record, generationKey, 0, 0),
		fence.transactionConditions()...,
	)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: entryRecordKey(record.Entry.ID), Value: primaryValue},
		{
			Type: MutationPut, Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID),
			Value: []byte(record.Entry.ID),
		},
		{Type: MutationPut, Key: generationKey, Value: generationValue},
		epochMutation,
	})
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return Versioned[EntryRecord]{}, classifyEntryWriteConflict(
			result.FailureReads, record, 0, 0, fence,
		)
	}
	return Versioned[EntryRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// CreateEntryIdempotent atomically commits desired metadata, its owner index,
// one immutable value generation, and the exact completed replay marker.
func (repository *EntryRepository) CreateEntryIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record EntryRecord,
	generation EntryValueGeneration,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateEntryHierarchy(ctx, environment, project, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Entry creation marker must be a completed direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	primaryValue, err := encodeEntryRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(record, generation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(generationValue)
	fence, _, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryRecordKey(record.Entry.ID),
			entryOwnerKey(record.EnvironmentID, record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(DeletionTargetEntry), record.Entry.ID),
		},
		-1,
		record.Entry.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	plan, err := newIdempotencyMutationPlan(
		append(
			entryWriteConditions(record, generationKey, 0, 0),
			fence.transactionConditions()...,
		),
		[]Mutation{
			{Type: MutationPut, Key: entryRecordKey(record.Entry.ID), Value: primaryValue},
			{
				Type: MutationPut, Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID),
				Value: []byte(record.Entry.ID),
			},
			{Type: MutationPut, Key: generationKey, Value: generationValue},
			epochMutation,
		},
		func(_ int64, values []*KeyValue) error {
			return classifyEntryWriteConflict(values, record, 0, 0, fence)
		},
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

func (repository *EntryRepository) GetEntry(
	ctx context.Context,
	id string,
) (Versioned[EntryRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EntryRecord]{}, err
	}
	if err := validateID(ids.KindEnvEntry, id); err != nil {
		return Versioned[EntryRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		entryRecordKey(id),
		id,
		errs.KindEntryNotFound,
		decodeEntryRecord,
		func(record EntryRecord) string { return record.Entry.ID },
	)
}

func (repository *EntryRepository) ListEntries(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[EntryRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[EntryRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"entries",
		"environment",
		environmentID,
		entryOwnerCollectionPrefix(environmentID),
		entryRecordKey,
		ids.KindEnvEntry,
		request,
		decodeEntryRecord,
		func(record EntryRecord) string { return record.Entry.ID },
		func(record EntryRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *EntryRepository) ReplaceEntry(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[EntryRecord],
	desired core.EnvEntry,
	valueGenerationID string,
	generation EntryValueGeneration,
) (Versioned[EntryRecord], error) {
	replacement, err := ReplaceEntryDesired(current.Record, desired, valueGenerationID)
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	if err := validateEntryHierarchy(ctx, environment, project, replacement); err != nil {
		return Versioned[EntryRecord]{}, err
	}
	if err := validateEntryVersion(current); err != nil {
		return Versioned[EntryRecord]{}, err
	}
	primaryValue, err := encodeEntryRecord(replacement)
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(replacement, generation)
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	defer clear(generationValue)
	fence, ownerRevision, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryRecordKey(current.Record.Entry.ID),
			entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
	)
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}

	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(
		entryWriteConditions(current.Record, generationKey, current.Revision, ownerRevision),
		fence.transactionConditions()...,
	)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: entryRecordKey(current.Record.Entry.ID), Value: primaryValue},
		{Type: MutationPut, Key: generationKey, Value: generationValue},
		epochMutation,
	})
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return Versioned[EntryRecord]{}, classifyEntryWriteConflict(
			result.FailureReads, current.Record, current.Revision, ownerRevision, fence,
		)
	}
	return Versioned[EntryRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// ReplaceEntryIdempotent atomically commits replacement desired metadata, one
// immutable value generation, and the exact completed replay marker.
func (repository *EntryRepository) ReplaceEntryIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[EntryRecord],
	desired core.EnvEntry,
	valueGenerationID string,
	generation EntryValueGeneration,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	replacement, err := ReplaceEntryDesired(current.Record, desired, valueGenerationID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryHierarchy(ctx, environment, project, replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEntryVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.Record.EnvironmentID ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != IdempotencyReplayTargetEntry ||
		marker.ReplayTarget.ID != current.Record.Entry.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Entry edit marker must be a completed Environment-scoped direct mutation for the target Entry",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	primaryValue, err := encodeEntryRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(primaryValue)
	generationKey, generationValue, err := prepareEntryGeneration(replacement, generation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(generationValue)
	fence, ownerRevision, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryRecordKey(current.Record.Entry.ID),
			entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			generationKey,
			deletionTombstoneKey(string(DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)
	plan, err := newIdempotencyMutationPlan(
		append(
			entryWriteConditions(current.Record, generationKey, current.Revision, ownerRevision),
			fence.transactionConditions()...,
		),
		[]Mutation{
			{Type: MutationPut, Key: entryRecordKey(current.Record.Entry.ID), Value: primaryValue},
			{Type: MutationPut, Key: generationKey, Value: generationValue},
			epochMutation,
		},
		func(_ int64, values []*KeyValue) error {
			return classifyEntryWriteConflict(
				values, current.Record, current.Revision, ownerRevision, fence,
			)
		},
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

func (repository *EntryRepository) DeleteEntry(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[EntryRecord],
) (int64, error) {
	if err := validateEntryHierarchy(ctx, environment, project, current.Record); err != nil {
		return 0, err
	}
	if err := validateEntryVersion(current); err != nil {
		return 0, err
	}
	fence, ownerRevision, err := repository.loadEntryMutationFence(
		ctx,
		environment,
		project,
		[]string{
			entryRecordKey(current.Record.Entry.ID),
			entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID),
			deletionTombstoneKey(string(DeletionTargetEntry), current.Record.Entry.ID),
		},
		1,
		current.Record.Entry.ID,
	)
	if err != nil {
		return 0, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return 0, err
	}
	defer clear(epochMutation.Value)
	conditions := append(
		entryDeleteConditions(current, ownerRevision),
		fence.transactionConditions()...,
	)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationDelete, Key: entryRecordKey(current.Record.Entry.ID)},
		{Type: MutationDelete, Key: entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID)},
		{
			Type: MutationDelete, Key: entryPlainValueGenerationPrefix + current.Record.Entry.ID + "/",
			Prefix: true,
		},
		{
			Type: MutationDelete, Key: entrySecretValueGenerationPrefix + current.Record.Entry.ID + "/",
			Prefix: true,
		},
		epochMutation,
	})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		defer clearKeyValues(result.FailureReads)
		return 0, classifyEntryDeleteConflict(result.FailureReads, current, ownerRevision, fence)
	}
	return result.Revision, nil
}

func prepareEntryGeneration(
	record EntryRecord,
	generation EntryValueGeneration,
) (string, []byte, error) {
	if (generation.Plain == nil) == (generation.Secret == nil) {
		return "", nil, errs.New(
			errs.KindValidationFailed,
			"Entry mutation must carry exactly one plain or secret value generation",
		)
	}
	if generation.Plain != nil {
		if record.Entry.Secret || generation.Plain.EnvironmentID != record.EnvironmentID ||
			generation.Plain.EntryID != record.Entry.ID ||
			generation.Plain.GenerationID != record.CurrentValueGenerationID {
			return "", nil, errs.New(errs.KindValidationFailed, "Entry plain value generation does not match metadata")
		}
		if record.Entry.Source.Kind == core.SourceLiteral &&
			!bytes.Equal(generation.Plain.Content, []byte(record.Entry.Source.Literal)) {
			return "", nil, errs.New(errs.KindValidationFailed, "Entry plain literal does not match its generation")
		}
		value, err := encodePlainEntryValueGeneration(*generation.Plain)
		return plainEntryValueGenerationKey(record.Entry.ID, record.CurrentValueGenerationID), value, err
	}
	if !record.Entry.Secret || generation.Secret.EnvironmentID != record.EnvironmentID ||
		generation.Secret.EntryID != record.Entry.ID ||
		generation.Secret.GenerationID != record.CurrentValueGenerationID {
		return "", nil, errs.New(errs.KindValidationFailed, "Entry secret value generation does not match metadata")
	}
	value, err := encodeSecretEntryValueGeneration(*generation.Secret)
	return secretEntryValueGenerationKey(record.Entry.ID, record.CurrentValueGenerationID), value, err
}

func (repository *EntryRepository) loadEntryMutationFence(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	domainKeys []string,
	ownerIndex int,
	entryID string,
) (environmentMutationFenceEvidence, int64, error) {
	keys := append([]string(nil), domainKeys...)
	environmentIndex := len(keys)
	keys = append(keys, environmentKey(environment.Record.ID))
	projectIndex := len(keys)
	keys = append(keys, projectKey(project.Record.ID))
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: keys,
	})
	if err != nil {
		return environmentMutationFenceEvidence{}, 0, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return environmentMutationFenceEvidence{}, 0, errs.New(
			errs.KindInternal,
			"Entry mutation fixed-revision evidence is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			return environmentMutationFenceEvidence{}, 0, errs.New(
				errs.KindInternal,
				"Entry mutation fixed-revision evidence is corrupt",
			)
		}
	}
	if result.Values[environmentIndex] == nil {
		return environmentMutationFenceEvidence{}, 0, errs.New(
			errs.KindEnvironmentNotFound,
			"Environment was not found",
		)
	}
	if result.Values[environmentIndex].ModRevision != environment.Revision {
		return environmentMutationFenceEvidence{}, 0, stateConflict("Environment", environment.Record.ID)
	}
	if result.Values[projectIndex] == nil {
		return environmentMutationFenceEvidence{}, 0, errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if result.Values[projectIndex].ModRevision != project.Revision {
		return environmentMutationFenceEvidence{}, 0, stateConflict("Project", project.Record.ID)
	}
	ownerRevision := int64(0)
	if ownerIndex >= 0 {
		if ownerIndex >= len(domainKeys) || result.Values[ownerIndex] == nil ||
			string(result.Values[ownerIndex].Value) != entryID {
			return environmentMutationFenceEvidence{}, 0, errs.New(
				errs.KindInternal,
				"Entry owner index is missing or corrupt",
			)
		}
		ownerRevision = result.Values[ownerIndex].ModRevision
	}
	fence, err := loadOrdinaryEnvironmentMutationFence(
		ctx,
		repository.store,
		environment.Record.ID,
		result.ReadRevision,
	)
	if err != nil {
		return environmentMutationFenceEvidence{}, 0, err
	}
	return fence, ownerRevision, nil
}

func entryWriteConditions(
	record EntryRecord,
	generationKey string,
	entryRevision int64,
	ownerRevision int64,
) []Condition {
	conditions := []Condition{
		{Key: entryRecordKey(record.Entry.ID), ModRevision: entryRevision},
		{Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID), ModRevision: ownerRevision},
		{Key: generationKey},
		{Key: deletionTombstoneKey(string(DeletionTargetEntry), record.Entry.ID)},
	}
	return conditions
}

func entryDeleteConditions(
	current Versioned[EntryRecord],
	ownerRevision int64,
) []Condition {
	conditions := []Condition{
		{Key: entryRecordKey(current.Record.Entry.ID), ModRevision: current.Revision},
		{Key: entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID), ModRevision: ownerRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetEntry), current.Record.Entry.ID)},
	}
	return conditions
}

func validateEntryHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record EntryRecord,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := validateProject(project.Record); err != nil {
		return err
	}
	if err := validateEntryRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision ||
		project.Revision <= 0 || project.ReadRevision < project.Revision ||
		record.EnvironmentID != environment.Record.ID || environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Entry hierarchy ownership or revisions are invalid")
	}
	return nil
}

func validateEntryVersion(current Versioned[EntryRecord]) error {
	if err := validateEntryRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Entry version metadata is invalid")
	}
	return nil
}

func classifyEntryWriteConflict(
	values []*KeyValue,
	record EntryRecord,
	expectedEntryRevision int64,
	expectedOwnerRevision int64,
	fence environmentMutationFenceEvidence,
) error {
	expected := 4 + len(fence.conditions)
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Entry write compare evidence is incomplete")
	}
	if expectedEntryRevision == 0 {
		if values[0] != nil || values[1] != nil {
			return errs.New(errs.KindStateConflict, "Entry stable identity is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindEntryNotFound, "Entry was not found")
		}
		if values[0].ModRevision != expectedEntryRevision {
			return stateConflict("entry", record.Entry.ID)
		}
		if values[1] == nil || string(values[1].Value) != record.Entry.ID {
			return errs.New(errs.KindInternal, "Entry owner index changed or is corrupt")
		}
		if values[1].ModRevision != expectedOwnerRevision {
			return stateConflict("Entry", record.Entry.ID)
		}
	}
	if values[2] != nil {
		return errs.New(errs.KindStateConflict, "Entry value generation id is already occupied")
	}
	if values[3] != nil {
		return errs.New(errs.KindResourceInUse, "Entry deletion is in progress")
	}
	if conflict := fence.classifyCAS(values[4:]); conflict != nil {
		return conflict
	}
	return stateConflict("entry", record.Entry.ID)
}

func classifyEntryDeleteConflict(
	values []*KeyValue,
	current Versioned[EntryRecord],
	expectedOwnerRevision int64,
	fence environmentMutationFenceEvidence,
) error {
	expected := 3 + len(fence.conditions)
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Entry delete compare evidence is incomplete")
	}
	if values[0] == nil {
		return errs.New(errs.KindEntryNotFound, "Entry was not found")
	}
	if values[0].ModRevision != current.Revision {
		return stateConflict("entry", current.Record.Entry.ID)
	}
	if values[1] == nil || string(values[1].Value) != current.Record.Entry.ID {
		return errs.New(errs.KindInternal, "Entry owner index changed or is corrupt")
	}
	if values[1].ModRevision != expectedOwnerRevision {
		return stateConflict("Entry", current.Record.Entry.ID)
	}
	if values[2] != nil {
		return errs.New(errs.KindResourceInUse, "Entry deletion is in progress")
	}
	if conflict := fence.classifyCAS(values[3:]); conflict != nil {
		return conflict
	}
	return stateConflict("entry", current.Record.Entry.ID)
}

func entryOwnerCollectionPrefix(environmentID string) string {
	return entryOwnerPrefix + environmentID + "/"
}

func entryOwnerKey(environmentID string, entryID string) string {
	return entryOwnerCollectionPrefix(environmentID) + entryID
}
