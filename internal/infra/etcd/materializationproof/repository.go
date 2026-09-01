package materializationproof

import (
	"bytes"
	"context"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	coreproof "github.com/AlanD20/groundplane/internal/core/materializationproof"
	base "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	proofPrefix                        = "/v1/records/materialization-proofs/"
	deletionTombstonePrefix            = "/v1/runtime/deletions/"
	proofGenerationDeletionFencePrefix = deletionTombstonePrefix + "materialization-proof-generation/"
)

type store interface {
	GetMany(context.Context, base.GetManyRequest) (*base.GetManyResult, error)
	Transact(context.Context, []base.Condition, []base.Mutation) (base.TransactionResult, error)
}

type Repository struct{ store store }

type Versioned struct {
	Proof        coreproof.Proof
	ModRevision  int64
	ReadRevision int64
}

func NewRepository(backend base.Store) (*Repository, error) { return newRepository(backend) }

func newRepository(backend store) (*Repository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "materialization proof store is required")
	}
	return &Repository{store: backend}, nil
}

// Publish creates one immutable proof under repository-derived semantic
// authorities. Exact replay precedes mutable applied/Task authority checks.
func (repository *Repository) Publish(ctx context.Context, proof coreproof.Proof) (Versioned, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned{}, err
	}
	if repository == nil || repository.store == nil {
		return Versioned{}, errs.New(errs.KindInternal, "materialization proof repository is not configured")
	}
	canonical, err := coreproof.Restore(proof.Record())
	if err != nil {
		return Versioned{}, err
	}
	encoded, err := encodeProof(canonical)
	if err != nil {
		return Versioned{}, err
	}
	defer clear(encoded)
	keys := publicationKeys(canonical)
	read, err := repository.store.GetMany(ctx, base.GetManyRequest{Keys: keys})
	if err != nil {
		return Versioned{}, err
	}
	if read == nil || read.ReadRevision <= 0 || read.ResponseRevision < read.ReadRevision || len(read.Values) != 5 {
		return Versioned{}, corruptProof()
	}
	defer clearKeyValues(read.Values)
	if replay, found, replayErr := exactReplay(
		canonical,
		encoded,
		keys,
		read.Values,
		read.ReadRevision,
	); found ||
		replayErr != nil {
		return replay, replayErr
	}
	if read.Values[1] != nil || read.Values[2] != nil {
		return Versioned{}, authorityConflict()
	}
	conditions, err := publicationConditions(canonical, keys, read.Values)
	if err != nil {
		return Versioned{}, err
	}
	result, err := repository.store.Transact(ctx, conditions, []base.Mutation{{
		Type: base.MutationPut, Key: keys[0], Value: encoded,
	}})
	if err != nil {
		return Versioned{}, err
	}
	defer clearKeyValues(result.FailureReads)
	if result.Succeeded {
		return Versioned{Proof: canonical, ModRevision: result.Revision, ReadRevision: result.Revision}, nil
	}
	if len(result.FailureReads) != 5 {
		return Versioned{}, corruptProof()
	}
	replayValues := []*base.KeyValue{
		result.FailureReads[0], result.FailureReads[3], result.FailureReads[4],
		result.FailureReads[1], result.FailureReads[2],
	}
	replayKeys := []string{keys[0], keys[1], keys[2], keys[3], keys[4]}
	if replay, found, replayErr := exactReplay(
		canonical, encoded, replayKeys, replayValues, result.Revision,
	); found || replayErr != nil {
		return replay, replayErr
	}
	return Versioned{}, authorityConflict()
}

func publicationKeys(proof coreproof.Proof) []string {
	environmentID := proof.EnvironmentID()
	return []string{
		proofKey(environmentID, proof.RenderGeneration()),
		environmentDeletionFenceKey(environmentID),
		proofGenerationDeletionFenceKey(environmentID, proof.RenderGeneration()),
		base.EnvironmentComposeProjectionStorageKey(environmentID),
		base.TaskStorageKey(proof.ProducingTaskID()),
	}
}

func exactReplay(
	proof coreproof.Proof,
	encoded []byte,
	keys []string,
	values []*base.KeyValue,
	readRevision int64,
) (Versioned, bool, error) {
	if len(keys) != 5 || len(values) != 5 {
		return Versioned{}, false, corruptProof()
	}
	if values[0] == nil {
		return Versioned{}, false, nil
	}
	if values[0].Key != keys[0] || values[0].ModRevision <= 0 || values[1] != nil || values[2] != nil {
		return Versioned{}, true, authorityConflict()
	}
	existing, err := decodeProof(values[0].Value)
	if err != nil {
		return Versioned{}, true, err
	}
	if !bytes.Equal(values[0].Value, encoded) || existing.EnvironmentID() != proof.EnvironmentID() ||
		existing.RenderGeneration() != proof.RenderGeneration() ||
		existing.CanonicalSHA256() != proof.CanonicalSHA256() {
		return Versioned{}, true, errs.New(
			errs.KindStateConflict,
			"materialization proof generation is already occupied",
		)
	}
	return Versioned{Proof: existing, ModRevision: values[0].ModRevision, ReadRevision: readRevision}, true, nil
}

func publicationConditions(
	proof coreproof.Proof,
	keys []string,
	values []*base.KeyValue,
) ([]base.Condition, error) {
	if values[3] == nil || values[4] == nil || values[3].Key != keys[3] || values[4].Key != keys[4] ||
		values[3].ModRevision <= 0 || values[4].ModRevision <= 0 {
		return nil, authorityConflict()
	}
	projection, err := base.DecodeEnvironmentComposeProjectionStorage(values[3].Value)
	if err != nil {
		return nil, corruptProof()
	}
	task, err := base.DecodeTaskStorageRecord(values[4].Value)
	if err != nil {
		return nil, corruptProof()
	}
	if err := validateSemanticAuthority(proof, projection, task); err != nil {
		return nil, err
	}
	return []base.Condition{
		{Key: keys[0]},
		{Key: keys[3], ModRevision: values[3].ModRevision},
		{Key: keys[4], ModRevision: values[4].ModRevision},
		{Key: keys[1]},
		{Key: keys[2]},
	}, nil
}

func validateSemanticAuthority(
	proof coreproof.Proof,
	projection base.EnvironmentComposeProjection,
	task base.TaskRecord,
) error {
	record := proof.Record()
	if projection.EnvironmentID != record.EnvironmentID || projection.RevisionID != record.AppliedRevisionID ||
		projection.RenderGeneration != record.RenderGeneration || task.ID != record.ProducingTaskID ||
		task.Executor != base.TaskExecutorAgent || uint64(task.RenderGeneration) != record.RenderGeneration ||
		task.Params[base.TaskMaterializationEnvironmentParam] != record.EnvironmentID ||
		task.Params[base.EnvironmentDesiredRevisionParam] != record.AppliedRevisionID {
		return authorityConflict()
	}
	services := make(map[string]string, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		services[service.Desired.ID] = service.Desired.Name
	}
	entries := make(map[string]string, len(projection.Entries))
	for _, entry := range projection.Entries {
		entries[entry.Entry.ID] = entry.CurrentValueGenerationID
	}
	references := make(map[string]base.TaskMaterializationRecord, len(task.Materializations))
	for _, reference := range task.Materializations {
		references[reference.MaterializationID] = reference
	}
	if len(references) != len(record.Members) {
		return authorityConflict()
	}
	for _, member := range record.Members {
		if member.ServiceID != "" && services[member.ServiceID] != member.ServiceName {
			return authorityConflict()
		}
		reference, found := references[member.MaterializationID]
		if !found || !memberMatchesTask(member, reference, entries) {
			return authorityConflict()
		}
	}
	return nil
}

func memberMatchesTask(
	member coreproof.MemberRecord,
	reference base.TaskMaterializationRecord,
	appliedEntries map[string]string,
) bool {
	if reference.EnvironmentID == "" || reference.Destination != member.Destination ||
		reference.ServiceID != member.ServiceID || reference.ServiceName != member.ServiceName ||
		reference.UID != member.UID || reference.GID != member.GID || reference.Mode != member.Mode ||
		reference.Length != member.Length || reference.SHA256 != member.ContentSHA256 ||
		!outputMatches(member.Outcome, member.OutputKind, reference.OutputKind) {
		return false
	}
	entries, secrets, sourcesValid := taskSources(reference.Source)
	if !sourcesValid || len(entries) != len(member.EntryGenerations) ||
		len(secrets) != len(member.ReusableSecrets) {
		return false
	}
	for _, entry := range member.EntryGenerations {
		stored, found := entries[entry.EntryID]
		if !found || stored.generation != entry.GenerationID || stored.storage != string(entry.Storage) ||
			appliedEntries[entry.EntryID] != entry.GenerationID {
			return false
		}
	}
	for _, secret := range member.ReusableSecrets {
		stored, found := secrets[secret.SecretID]
		if !found || stored.Revision != secret.MetadataRevision ||
			stored.CiphertextSHA256 != secret.CiphertextSHA256 {
			return false
		}
	}
	return true
}

type taskEntrySource struct {
	generation string
	storage    string
}

func taskSources(source base.TaskMaterializationSource) (
	map[string]taskEntrySource,
	map[string]base.TaskSecretValueReference,
	bool,
) {
	entries := make(map[string]taskEntrySource)
	secrets := make(map[string]base.TaskSecretValueReference)
	valid := true
	addEntry := func(value base.TaskEntryValueReference) {
		candidate := taskEntrySource{
			generation: value.ValueGenerationID, storage: string(value.Storage),
		}
		if existing, found := entries[value.EntryID]; found && existing != candidate {
			valid = false
			return
		}
		entries[value.EntryID] = candidate
	}
	if source.EntryValue != nil {
		addEntry(*source.EntryValue)
	}
	if source.GeneratedEnvironment != nil {
		for _, value := range source.GeneratedEnvironment.Values {
			if value.Secret != nil {
				if existing, found := secrets[value.Secret.SecretID]; found &&
					(existing.Revision != value.Secret.Revision ||
						existing.CiphertextSHA256 != value.Secret.CiphertextSHA256) {
					valid = false
					continue
				}
				secrets[value.Secret.SecretID] = *value.Secret
			} else {
				addEntry(value.Value)
			}
		}
	}
	return entries, secrets, valid
}

func outputMatches(
	outcome coreproof.Outcome,
	kind coreproof.OutputKind,
	taskKind base.TaskMaterializationOutputKind,
) bool {
	if outcome == coreproof.OutcomePresent {
		return (kind == coreproof.OutputGeneratedEnvironment &&
			taskKind == base.TaskMaterializationOutputGeneratedEnvironment) ||
			(kind == coreproof.OutputPlainFile && taskKind == base.TaskMaterializationOutputPlainFile) ||
			(kind == coreproof.OutputSecretFile && taskKind == base.TaskMaterializationOutputSecretFile)
	}
	return (kind == coreproof.OutputGeneratedEnvironment &&
		taskKind == base.TaskMaterializationOutputRemoveGeneratedEnv) ||
		(kind == coreproof.OutputPlainFile && taskKind == base.TaskMaterializationOutputRemovePlainFile) ||
		(kind == coreproof.OutputSecretFile && taskKind == base.TaskMaterializationOutputRemoveSecretFile)
}

func authorityConflict() error {
	return errs.New(errs.KindStateConflict, "materialization proof publication authority changed")
}

func (repository *Repository) LoadExact(
	ctx context.Context,
	environmentID string,
	renderGeneration uint64,
	revision int64,
) (Versioned, bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned{}, false, err
	}
	if repository == nil || repository.store == nil {
		return Versioned{}, false, errs.New(errs.KindInternal, "materialization proof repository is not configured")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || renderGeneration == 0 || revision <= 0 {
		return Versioned{}, false, errs.New(errs.KindValidationFailed, "materialization proof lookup is invalid")
	}
	key := proofKey(environmentID, renderGeneration)
	result, err := repository.store.GetMany(ctx, base.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Versioned{}, false, err
	}
	if result == nil || result.ReadRevision != revision || result.ResponseRevision < revision ||
		len(result.Values) != 1 {
		return Versioned{}, false, corruptProof()
	}
	defer clearKeyValues(result.Values)
	if result.Values[0] == nil {
		return Versioned{ReadRevision: revision}, false, nil
	}
	value := result.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
		return Versioned{}, false, corruptProof()
	}
	loaded, err := decodeProof(value.Value)
	if err != nil {
		return Versioned{}, false, err
	}
	if loaded.EnvironmentID() != environmentID || loaded.RenderGeneration() != renderGeneration {
		return Versioned{}, false, corruptProof()
	}
	return Versioned{Proof: loaded, ModRevision: value.ModRevision, ReadRevision: revision}, true, nil
}

func proofKey(environmentID string, renderGeneration uint64) string {
	return proofPrefix + environmentID + "/" + strconv.FormatUint(renderGeneration, 10)
}

func environmentDeletionFenceKey(environmentID string) string {
	return deletionTombstonePrefix + "environment/" + environmentID
}

func proofGenerationDeletionFenceKey(environmentID string, renderGeneration uint64) string {
	return proofGenerationDeletionFencePrefix + environmentID + "/" + strconv.FormatUint(renderGeneration, 10)
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errs.New(errs.KindValidationFailed, "context is required")
	}
	return ctx.Err()
}

func clearKeyValues(values []*base.KeyValue) {
	for _, value := range values {
		if value != nil {
			clear(value.Value)
		}
	}
}
