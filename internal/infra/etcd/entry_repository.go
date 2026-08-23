package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryOwnerPrefix = "/v1/indexes/entries/by-owner/environment/"

// EntryValueGeneration is the closed atomic value input for an Entry mutation.
type EntryValueGeneration struct {
	Plain  *PlainEntryValueGeneration
	Secret *SecretEntryValueGeneration
}

type EntryRepository struct {
	store hierarchyStore
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

	conditions := entryWriteConditions(environment, project, record, generationKey, 0, 0)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: entryRecordKey(record.Entry.ID), Value: primaryValue},
		{
			Type: MutationPut, Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID),
			Value: []byte(record.Entry.ID),
		},
		{Type: MutationPut, Key: generationKey, Value: generationValue},
	})
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[EntryRecord]{}, classifyEntryWriteConflict(
			result.FailureReads, environment, project, record, 0,
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
	plan, err := newIdempotencyMutationPlan(
		entryWriteConditions(environment, project, record, generationKey, 0, 0),
		[]Mutation{
			{Type: MutationPut, Key: entryRecordKey(record.Entry.ID), Value: primaryValue},
			{
				Type: MutationPut, Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID),
				Value: []byte(record.Entry.ID),
			},
			{Type: MutationPut, Key: generationKey, Value: generationValue},
		},
		func(_ int64, values []*KeyValue) error {
			return classifyEntryWriteConflict(values, environment, project, record, 0)
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
	owner, err := repository.entryOwnerAtCurrentRevision(ctx, current)
	if err != nil {
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

	conditions := entryWriteConditions(
		environment, project, current.Record, generationKey, current.Revision, owner.ModRevision,
	)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: entryRecordKey(current.Record.Entry.ID), Value: primaryValue},
		{Type: MutationPut, Key: generationKey, Value: generationValue},
	})
	if err != nil {
		return Versioned[EntryRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[EntryRecord]{}, classifyEntryWriteConflict(
			result.FailureReads, environment, project, current.Record, current.Revision,
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
	owner, err := repository.entryOwnerAtCurrentRevision(ctx, current)
	if err != nil {
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
	plan, err := newIdempotencyMutationPlan(
		entryWriteConditions(
			environment, project, current.Record, generationKey, current.Revision, owner.ModRevision,
		),
		[]Mutation{
			{Type: MutationPut, Key: entryRecordKey(current.Record.Entry.ID), Value: primaryValue},
			{Type: MutationPut, Key: generationKey, Value: generationValue},
		},
		func(_ int64, values []*KeyValue) error {
			return classifyEntryWriteConflict(
				values, environment, project, current.Record, current.Revision,
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
	owner, err := repository.entryOwnerAtCurrentRevision(ctx, current)
	if err != nil {
		return 0, err
	}
	conditions := entryDeleteConditions(environment, project, current, owner.ModRevision)
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
	})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, classifyEntryDeleteConflict(result.FailureReads, environment, project, current)
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

func (repository *EntryRepository) entryOwnerAtCurrentRevision(
	ctx context.Context,
	current Versioned[EntryRecord],
) (*KeyValue, error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID)},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return nil, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil ||
		string(result.Values[0].Value) != current.Record.Entry.ID {
		return nil, errs.New(errs.KindInternal, "Entry owner index is missing or corrupt")
	}
	return result.Values[0], nil
}

func entryWriteConditions(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record EntryRecord,
	generationKey string,
	entryRevision int64,
	ownerRevision int64,
) []Condition {
	conditions := []Condition{
		{Key: entryRecordKey(record.Entry.ID), ModRevision: entryRevision},
		{Key: entryOwnerKey(record.EnvironmentID, record.Entry.ID), ModRevision: ownerRevision},
		{Key: generationKey},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("entry", record.Entry.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	return conditions
}

func entryDeleteConditions(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[EntryRecord],
	ownerRevision int64,
) []Condition {
	conditions := []Condition{
		{Key: entryRecordKey(current.Record.Entry.ID), ModRevision: current.Revision},
		{Key: entryOwnerKey(current.Record.EnvironmentID, current.Record.Entry.ID), ModRevision: ownerRevision},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("entry", current.Record.Entry.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
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
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record EntryRecord,
	expectedEntryRevision int64,
) error {
	expected := 8
	if project.Record.TenantID != "" {
		expected++
	}
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
	}
	if values[2] != nil {
		return errs.New(errs.KindStateConflict, "Entry value generation id is already occupied")
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[3].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	for _, index := range []int{5, 6, 7} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Entry hierarchy deletion is in progress")
		}
	}
	if expected == 9 && values[8] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return stateConflict("entry", record.Entry.ID)
}

func classifyEntryDeleteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[EntryRecord],
) error {
	expected := 7
	if project.Record.TenantID != "" {
		expected++
	}
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
	if values[2] == nil || values[2].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[3] == nil || values[3].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	for _, index := range []int{4, 5, 6} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Entry hierarchy deletion is in progress")
		}
	}
	if expected == 8 && values[7] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return stateConflict("entry", current.Record.Entry.ID)
}

func entryOwnerCollectionPrefix(environmentID string) string {
	return entryOwnerPrefix + environmentID + "/"
}

func entryOwnerKey(environmentID string, entryID string) string {
	return entryOwnerCollectionPrefix(environmentID) + entryID
}
