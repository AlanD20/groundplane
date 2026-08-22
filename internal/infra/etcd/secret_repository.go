package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	secretOwnerIndexPrefix = "/v1/indexes/secrets/by-owner/"
	secretKeyIndexPrefix   = "/v1/indexes/secrets/by-key/"
)

// SecretOwner is a closed project-or-platform ownership input. A nil Project
// denotes platform scope.
type SecretOwner struct {
	Project *Versioned[ProjectRecord]
}

func ProjectSecretOwner(project Versioned[ProjectRecord]) SecretOwner {
	return SecretOwner{Project: &project}
}

func PlatformSecretOwner() SecretOwner { return SecretOwner{} }

type SecretRepository struct {
	store hierarchyStore
}

func NewSecretRepository(store Store) (*SecretRepository, error) {
	return newSecretRepository(store)
}

func newSecretRepository(store hierarchyStore) (*SecretRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Secret store is required")
	}
	return &SecretRepository{store: store}, nil
}

func (repository *SecretRepository) CreateSecret(
	ctx context.Context,
	owner SecretOwner,
	record SecretRecord,
	value SecretEncryptedValue,
) (Versioned[SecretRecord], error) {
	if err := validateSecretOwnership(ctx, owner, record); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if err := validateSecretValueBinding(record, value); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	primaryValue, err := encodeSecretRecord(record)
	if err != nil {
		return Versioned[SecretRecord]{}, err
	}
	defer clear(primaryValue)
	encryptedValue, err := encodeSecretEncryptedValue(value)
	if err != nil {
		return Versioned[SecretRecord]{}, err
	}
	defer clear(encryptedValue)

	conditions := secretCreateConditions(owner, record)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: secretRecordKey(record.Secret.ID), Value: primaryValue},
		{Type: MutationPut, Key: secretOwnerKey(record.Secret), Value: []byte(record.Secret.ID)},
		{Type: MutationPut, Key: secretScopedKey(record.Secret), Value: []byte(record.Secret.ID)},
		{Type: MutationPut, Key: secretValueKey(record.Secret.ID), Value: encryptedValue},
	})
	if err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[SecretRecord]{}, classifySecretCreateConflict(
			result.FailureReads, owner, record,
		)
	}
	return Versioned[SecretRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// CreateSecretIdempotent atomically commits the redacted metadata, ownership
// indexes, encrypted value, and exact completed replay marker.
func (repository *SecretRepository) CreateSecretIdempotent(
	ctx context.Context,
	owner SecretOwner,
	record SecretRecord,
	value SecretEncryptedValue,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateSecretOwnership(ctx, owner, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateSecretValueBinding(record, value); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Secret creation marker must be a completed direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	primaryValue, err := encodeSecretRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(primaryValue)
	encryptedValue, err := encodeSecretEncryptedValue(value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(encryptedValue)
	plan, err := newIdempotencyMutationPlan(
		secretCreateConditions(owner, record),
		[]Mutation{
			{Type: MutationPut, Key: secretRecordKey(record.Secret.ID), Value: primaryValue},
			{Type: MutationPut, Key: secretOwnerKey(record.Secret), Value: []byte(record.Secret.ID)},
			{Type: MutationPut, Key: secretScopedKey(record.Secret), Value: []byte(record.Secret.ID)},
			{Type: MutationPut, Key: secretValueKey(record.Secret.ID), Value: encryptedValue},
		},
		func(_ int64, values []*KeyValue) error {
			return classifySecretCreateConflict(values, owner, record)
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

func (repository *SecretRepository) GetSecret(
	ctx context.Context,
	id string,
) (Versioned[SecretRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if err := validateID(ids.KindSecret, id); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	current, err := getRecord(
		ctx,
		repository.store,
		secretRecordKey(id),
		id,
		errs.KindSecretNotFound,
		decodeSecretRecord,
		func(record SecretRecord) string { return record.Secret.ID },
	)
	if err != nil {
		return Versioned[SecretRecord]{}, err
	}
	tombstones, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{deletionTombstoneKey(string(DeletionTargetSecret), id)},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if tombstones == nil || len(tombstones.Values) != 1 {
		return Versioned[SecretRecord]{}, errs.New(errs.KindInternal, "Secret deletion fence read is incomplete")
	}
	if tombstones.Values[0] != nil {
		if err := validateSecretDeletionFence(tombstones.Values[0], id); err != nil {
			return Versioned[SecretRecord]{}, err
		}
		return Versioned[SecretRecord]{}, errs.New(errs.KindSecretNotFound, "Secret was not found")
	}
	return current, nil
}

func (repository *SecretRepository) GetSecretValue(
	ctx context.Context,
	current Versioned[SecretRecord],
) (SecretEncryptedValue, error) {
	if err := validateSecretVersion(current); err != nil {
		return SecretEncryptedValue{}, err
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{secretValueKey(current.Record.Secret.ID)}, Revision: current.ReadRevision,
	})
	if err != nil {
		return SecretEncryptedValue{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return SecretEncryptedValue{}, errs.New(errs.KindInternal, "Secret encrypted value is missing")
	}
	value, err := decodeSecretEncryptedValue(result.Values[0].Value)
	if err != nil || value.SecretID != current.Record.Secret.ID {
		clear(value.Ciphertext)
		return SecretEncryptedValue{}, corruptSecretRecord()
	}
	return value, nil
}

func (repository *SecretRepository) ListSecrets(
	ctx context.Context,
	scope core.SecretScope,
	projectID string,
	request PageRequest,
) (Page[SecretRecord], error) {
	if err := validateSecretListScope(scope, projectID); err != nil {
		return Page[SecretRecord]{}, err
	}
	ownerKind, ownerID := secretScopeKey(scope, projectID)
	page, err := listIndexPage(
		ctx,
		repository.store,
		"secrets",
		ownerKind,
		ownerID,
		secretOwnerCollectionPrefix(scope, projectID),
		secretRecordKey,
		ids.KindSecret,
		request,
		decodeSecretRecord,
		func(record SecretRecord) string { return record.Secret.ID },
		func(record SecretRecord) bool {
			return record.Secret.Scope == scope && record.Secret.ProjectID == projectID
		},
	)
	if err != nil || len(page.Items) == 0 {
		return page, err
	}
	keys := make([]string, len(page.Items))
	for index, item := range page.Items {
		keys[index] = deletionTombstoneKey(string(DeletionTargetSecret), item.Record.Secret.ID)
	}
	tombstones, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: page.Revision})
	if err != nil {
		return Page[SecretRecord]{}, err
	}
	if tombstones == nil || tombstones.ReadRevision != page.Revision || len(tombstones.Values) != len(keys) {
		return Page[SecretRecord]{}, errs.New(errs.KindInternal, "Secret deletion fence page is incomplete")
	}
	visible := make([]Versioned[SecretRecord], 0, len(page.Items))
	for index, item := range page.Items {
		if tombstones.Values[index] == nil {
			visible = append(visible, item)
			continue
		}
		if err := validateSecretDeletionFence(tombstones.Values[index], item.Record.Secret.ID); err != nil {
			return Page[SecretRecord]{}, err
		}
	}
	page.Items = visible
	return page, nil
}

// ResolveSecret accepts either an in-scope stable id or a key. Key lookup is
// project-first with platform fallback at one fixed etcd revision.
func (repository *SecretRepository) ResolveSecret(
	ctx context.Context,
	projectID string,
	reference string,
) (Versioned[SecretRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if err := validateID(ids.KindProject, projectID); err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if reference == "" {
		return Versioned[SecretRecord]{}, errs.New(errs.KindValidationFailed, "Secret reference is required")
	}
	if ids.Validate(ids.KindSecret, reference) == nil {
		record, err := repository.GetSecret(ctx, reference)
		if err != nil {
			return Versioned[SecretRecord]{}, err
		}
		if record.Record.Secret.Scope == core.SecretScopePlatform ||
			record.Record.Secret.ProjectID == projectID {
			return record, nil
		}
		return Versioned[SecretRecord]{}, errs.New(errs.KindSecretNotFound, "Secret was not found in scope")
	}

	indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		secretKeyIndexKey(core.SecretScopeProject, projectID, reference),
		secretKeyIndexKey(core.SecretScopePlatform, "", reference),
	}})
	if err != nil {
		return Versioned[SecretRecord]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 {
		return Versioned[SecretRecord]{}, errs.New(errs.KindInternal, "Secret fallback index read is incomplete")
	}
	for index, selected := range indexes.Values {
		if selected == nil {
			continue
		}
		id := string(selected.Value)
		if ids.Validate(ids.KindSecret, id) != nil {
			return Versioned[SecretRecord]{}, errs.New(errs.KindInternal, "Secret key index is corrupt")
		}
		stored, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{
				secretRecordKey(id),
				deletionTombstoneKey(string(DeletionTargetSecret), id),
			},
			Revision: indexes.ReadRevision,
		})
		if err != nil {
			return Versioned[SecretRecord]{}, err
		}
		if stored == nil || stored.ReadRevision != indexes.ReadRevision || len(stored.Values) != 2 ||
			stored.Values[0] == nil {
			return Versioned[SecretRecord]{}, errs.New(
				errs.KindInternal,
				"Secret key index references a missing record",
			)
		}
		if stored.Values[1] != nil {
			if err := validateSecretDeletionFence(stored.Values[1], id); err != nil {
				return Versioned[SecretRecord]{}, err
			}
			continue
		}
		record, err := decodeSecretRecord(stored.Values[0].Value)
		projectMatch := index == 0 && record.Secret.Scope == core.SecretScopeProject &&
			record.Secret.ProjectID == projectID
		platformMatch := index == 1 && record.Secret.Scope == core.SecretScopePlatform &&
			record.Secret.ProjectID == ""
		if err != nil || record.Secret.ID != id || record.Secret.Key != reference ||
			(!projectMatch && !platformMatch) {
			return Versioned[SecretRecord]{}, corruptSecretRecord()
		}
		return Versioned[SecretRecord]{
			Record: record, Revision: stored.Values[0].ModRevision, ReadRevision: stored.ReadRevision,
		}, nil
	}
	return Versioned[SecretRecord]{}, errs.New(errs.KindSecretNotFound, "Secret was not found in scope")
}

func validateSecretDeletionFence(value *KeyValue, secretID string) error {
	if value == nil {
		return errs.New(errs.KindInternal, "Secret deletion fence is missing")
	}
	tombstone, err := decodeDeletionTombstone(value.Value)
	if err != nil || tombstone.TargetKind != DeletionTargetSecret || tombstone.TargetID != secretID ||
		tombstone.Phase != DeletionPhaseFinalizing {
		return corruptDeletionTombstone()
	}
	return nil
}

func (repository *SecretRepository) DeleteSecret(
	ctx context.Context,
	owner SecretOwner,
	current Versioned[SecretRecord],
) (int64, error) {
	if err := validateSecretOwnership(ctx, owner, current.Record); err != nil {
		return 0, err
	}
	if err := validateSecretVersion(current); err != nil {
		return 0, err
	}
	dependencies, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			secretOwnerKey(current.Record.Secret),
			secretScopedKey(current.Record.Secret),
			secretValueKey(current.Record.Secret.ID),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return 0, err
	}
	if dependencies == nil || len(dependencies.Values) != 3 ||
		dependencies.Values[0] == nil || dependencies.Values[1] == nil || dependencies.Values[2] == nil ||
		string(dependencies.Values[0].Value) != current.Record.Secret.ID ||
		string(dependencies.Values[1].Value) != current.Record.Secret.ID {
		return 0, errs.New(errs.KindInternal, "Secret indexes or encrypted value are missing")
	}
	conditions := secretDeleteConditions(owner, current, dependencies.Values)
	result, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationDelete, Key: secretRecordKey(current.Record.Secret.ID)},
		{Type: MutationDelete, Key: secretOwnerKey(current.Record.Secret)},
		{Type: MutationDelete, Key: secretScopedKey(current.Record.Secret)},
		{Type: MutationDelete, Key: secretValueKey(current.Record.Secret.ID)},
	})
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "Secret changed while deletion was requested")
	}
	return result.Revision, nil
}

func validateSecretValueBinding(record SecretRecord, value SecretEncryptedValue) error {
	if err := validateSecretEncryptedValue(value); err != nil {
		return err
	}
	if record.Secret.ID != value.SecretID {
		return errs.New(errs.KindValidationFailed, "Secret encrypted value does not match metadata")
	}
	return nil
}

func validateSecretOwnership(ctx context.Context, owner SecretOwner, record SecretRecord) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateSecretRecord(record); err != nil {
		return err
	}
	if owner.Project == nil {
		if record.Secret.Scope != core.SecretScopePlatform || record.Secret.ProjectID != "" {
			return errs.New(errs.KindValidationFailed, "Secret platform ownership is invalid")
		}
		return nil
	}
	if err := validateProject(owner.Project.Record); err != nil {
		return err
	}
	if owner.Project.Revision <= 0 || owner.Project.ReadRevision < owner.Project.Revision ||
		record.Secret.Scope != core.SecretScopeProject ||
		record.Secret.ProjectID != owner.Project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Secret project ownership or revision is invalid")
	}
	return nil
}

func validateSecretVersion(current Versioned[SecretRecord]) error {
	if err := validateSecretRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Secret version metadata is invalid")
	}
	return nil
}

func validateSecretListScope(scope core.SecretScope, projectID string) error {
	switch scope {
	case core.SecretScopeProject:
		return validateID(ids.KindProject, projectID)
	case core.SecretScopePlatform:
		if projectID != "" {
			return errs.New(errs.KindValidationFailed, "Platform Secret list must not set project id")
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "Secret list scope is invalid")
	}
}

func secretCreateConditions(owner SecretOwner, record SecretRecord) []Condition {
	conditions := []Condition{
		{Key: secretRecordKey(record.Secret.ID)},
		{Key: secretOwnerKey(record.Secret)},
		{Key: secretScopedKey(record.Secret)},
		{Key: secretValueKey(record.Secret.ID)},
	}
	if owner.Project != nil {
		conditions = append(conditions,
			Condition{Key: projectKey(owner.Project.Record.ID), ModRevision: owner.Project.Revision},
		)
	}
	conditions = append(conditions, Condition{Key: deletionTombstoneKey("secret", record.Secret.ID)})
	if owner.Project != nil {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("project", owner.Project.Record.ID)})
		if owner.Project.Record.TenantID != "" {
			conditions = append(
				conditions,
				Condition{Key: deletionTombstoneKey("tenant", owner.Project.Record.TenantID)},
			)
		}
	}
	return conditions
}

func secretDeleteConditions(
	owner SecretOwner,
	current Versioned[SecretRecord],
	dependencies []*KeyValue,
) []Condition {
	conditions := []Condition{
		{Key: secretRecordKey(current.Record.Secret.ID), ModRevision: current.Revision},
		{Key: secretOwnerKey(current.Record.Secret), ModRevision: dependencies[0].ModRevision},
		{Key: secretScopedKey(current.Record.Secret), ModRevision: dependencies[1].ModRevision},
		{Key: secretValueKey(current.Record.Secret.ID), ModRevision: dependencies[2].ModRevision},
	}
	if owner.Project != nil {
		conditions = append(conditions,
			Condition{Key: projectKey(owner.Project.Record.ID), ModRevision: owner.Project.Revision},
		)
	}
	conditions = append(
		conditions,
		Condition{Key: deletionTombstoneKey("secret", current.Record.Secret.ID)},
	)
	if owner.Project != nil {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("project", owner.Project.Record.ID)})
		if owner.Project.Record.TenantID != "" {
			conditions = append(
				conditions,
				Condition{Key: deletionTombstoneKey("tenant", owner.Project.Record.TenantID)},
			)
		}
	}
	return conditions
}

func classifySecretCreateConflict(
	values []*KeyValue,
	owner SecretOwner,
	record SecretRecord,
) error {
	expected := 5
	if owner.Project != nil {
		expected += 2
		if owner.Project.Record.TenantID != "" {
			expected++
		}
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Secret create compare evidence is incomplete")
	}
	if values[0] != nil || values[1] != nil || values[3] != nil {
		return errs.New(errs.KindStateConflict, "Secret stable identity is already in use")
	}
	if values[2] != nil {
		return errs.New(errs.KindNameConflict, "Secret key is already defined in this scope")
	}
	position := 4
	if owner.Project != nil {
		if values[position] == nil {
			return errs.New(errs.KindProjectNotFound, "Secret project was not found")
		}
		if values[position].ModRevision != owner.Project.Revision {
			return stateConflict("project", owner.Project.Record.ID)
		}
		position++
	}
	if values[position] != nil {
		return errs.New(errs.KindResourceInUse, "Secret deletion is in progress")
	}
	position++
	for ; position < len(values); position++ {
		if values[position] != nil {
			return errs.New(errs.KindResourceInUse, "Secret owner deletion is in progress")
		}
	}
	return stateConflict("secret", record.Secret.ID)
}

func secretScopeKey(scope core.SecretScope, projectID string) (string, string) {
	if scope == core.SecretScopeProject {
		return "project", projectID
	}
	return "platform", "-"
}

func secretOwnerCollectionPrefix(scope core.SecretScope, projectID string) string {
	kind, id := secretScopeKey(scope, projectID)
	return secretOwnerIndexPrefix + kind + "/" + id + "/"
}

func secretOwnerKey(secret core.Secret) string {
	return secretOwnerCollectionPrefix(secret.Scope, secret.ProjectID) + secret.ID
}

func secretKeyIndexKey(scope core.SecretScope, projectID string, key string) string {
	kind, id := secretScopeKey(scope, projectID)
	return secretKeyIndexPrefix + kind + "/" + id + "/" + encodeDynamicSegment(key)
}

func secretScopedKey(secret core.Secret) string {
	return secretKeyIndexKey(secret.Scope, secret.ProjectID, secret.Key)
}
