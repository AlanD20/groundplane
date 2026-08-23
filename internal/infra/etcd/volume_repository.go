package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VolumeRepository owns Volume records, Environment membership, and scoped
// name uniqueness. Filesystem and Compose reconciliation remain Task concerns.
type VolumeRepository struct{ store hierarchyStore }

const (
	volumeEvidencePrimary = iota
	volumeEvidenceOwner
	volumeEvidenceName
	volumeEvidenceEnvironment
	volumeEvidenceProject
	volumeEvidenceTargetDeletion
	volumeEvidenceEnvironmentDeletion
	volumeEvidenceProjectDeletion
	volumeEvidenceTenantDeletion
	volumeBaseEvidenceCount = volumeEvidenceTenantDeletion
)

func NewVolumeRepository(store Store) (*VolumeRepository, error) { return newVolumeRepository(store) }

func newVolumeRepository(store hierarchyStore) (*VolumeRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "volume store is required")
	}
	return &VolumeRepository{store: store}, nil
}

func (repository *VolumeRepository) CreateVolume(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record VolumeRecord,
) (Versioned[VolumeRecord], error) {
	conditions, mutations, classify, err := prepareVolumeCreation(ctx, environment, project, record)
	if err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	defer clearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[VolumeRecord]{}, classify(result.Revision, result.FailureReads)
	}
	return Versioned[VolumeRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *VolumeRepository) CreateVolumeIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record VolumeRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateVolumeMutationMarker(marker, record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, mutations, classify, err := prepareVolumeCreation(ctx, environment, project, record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func prepareVolumeCreation(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record VolumeRecord,
) ([]Condition, []Mutation, idempotencyPlanClassifier, error) {
	if err := validateVolumeHierarchy(ctx, environment, project, record); err != nil {
		return nil, nil, nil, err
	}
	value, err := encodeVolumeRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	conditions := volumeWriteConditions(environment, project, record)
	mutations := []Mutation{
		{Type: MutationPut, Key: volumeKey(record.ID), Value: value},
		{
			Type:  MutationPut,
			Key:   volumeOwnerKey(record.EnvironmentID, record.ID),
			Value: []byte(record.ID),
		},
		{
			Type:  MutationPut,
			Key:   volumeNameKey(record.EnvironmentID, record.Name),
			Value: []byte(record.ID),
		},
	}
	classify := func(_ int64, values []*KeyValue) error {
		return classifyVolumeWriteConflict(values, environment, project, record)
	}
	return conditions, mutations, classify, nil
}

func (repository *VolumeRepository) GetVolume(
	ctx context.Context,
	id string,
) (Versioned[VolumeRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	if err := validateID(ids.KindVolume, id); err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, volumeKey(id), id, errs.KindVolumeNotFound, decodeVolumeRecord,
		func(record VolumeRecord) string { return record.ID },
	)
}

// GetVolumeByName resolves the current scoped label to a stable Volume at one
// MVCC revision. The owner index is verified rather than trusting one pointer.
func (repository *VolumeRepository) GetVolumeByName(
	ctx context.Context,
	environmentID string,
	name string,
) (Versioned[VolumeRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	if err := validateVolumeName(name); err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	index, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{volumeNameKey(environmentID, name)},
	})
	if err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	if index == nil || len(index.Values) != 1 {
		return Versioned[VolumeRecord]{}, errs.New(errs.KindInternal, "volume name lookup returned invalid evidence")
	}
	if index.Values[0] == nil {
		return Versioned[VolumeRecord]{}, errs.New(errs.KindVolumeNotFound, "volume was not found")
	}
	id := string(index.Values[0].Value)
	if err := validateID(ids.KindVolume, id); err != nil {
		return Versioned[VolumeRecord]{}, errs.New(errs.KindInternal, "volume name index contains an invalid id")
	}
	resolved, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			volumeKey(id),
			volumeOwnerKey(environmentID, id),
			volumeNameKey(environmentID, name),
		},
		Revision: index.ReadRevision,
	})
	if err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	if resolved == nil || len(resolved.Values) != 3 || resolved.Values[0] == nil ||
		resolved.Values[1] == nil || resolved.Values[2] == nil ||
		string(resolved.Values[1].Value) != id || string(resolved.Values[2].Value) != id {
		return Versioned[VolumeRecord]{}, errs.New(errs.KindInternal, "volume indexes are missing or corrupt")
	}
	record, err := decodeVolumeRecord(resolved.Values[0].Value)
	if err != nil {
		return Versioned[VolumeRecord]{}, err
	}
	if record.ID != id || record.EnvironmentID != environmentID || record.Name != name {
		return Versioned[VolumeRecord]{}, errs.New(errs.KindInternal, "volume name index does not match its record")
	}
	return Versioned[VolumeRecord]{
		Record: record, Revision: resolved.Values[0].ModRevision, ReadRevision: resolved.ReadRevision,
	}, nil
}

func (repository *VolumeRepository) ListVolumes(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[VolumeRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[VolumeRecord]{}, err
	}
	return listIndexPage(
		ctx, repository.store, "volumes", "environment", environmentID,
		volumeOwnerPrefix(environmentID), volumeKey, ids.KindVolume, request, decodeVolumeRecord,
		func(record VolumeRecord) string { return record.ID },
		func(record VolumeRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func validateVolumeMutationMarker(marker IdempotencyMarker, environmentID string) error {
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(
			errs.KindValidationFailed,
			"volume mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	return validateIdempotencyMarker(marker)
}

func volumeWriteConditions(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record VolumeRecord,
) []Condition {
	conditions := []Condition{
		{Key: volumeKey(record.ID)},
		{Key: volumeOwnerKey(record.EnvironmentID, record.ID)},
		{Key: volumeNameKey(record.EnvironmentID, record.Name)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("volume", record.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{
			Key: deletionTombstoneKey("tenant", project.Record.TenantID),
		})
	}
	return conditions
}

func validateVolumeHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record VolumeRecord,
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
	if err := validateVolumeRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision ||
		project.Revision <= 0 || project.ReadRevision < project.Revision ||
		record.EnvironmentID != environment.Record.ID || environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "volume hierarchy is invalid")
	}
	if environment.Record.ProvisioningState != EnvironmentProvisioningReady {
		return errs.New(errs.KindStateConflict, "Environment is not ready for Volume creation")
	}
	return nil
}

func classifyVolumeWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record VolumeRecord,
) error {
	expected := volumeBaseEvidenceCount
	if project.Record.TenantID != "" {
		expected++
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "volume write compare evidence is incomplete")
	}
	if values[volumeEvidencePrimary] != nil || values[volumeEvidenceOwner] != nil {
		return errs.New(errs.KindStateConflict, "volume stable identity is already in use")
	}
	if values[volumeEvidenceName] != nil {
		return errs.New(errs.KindNameConflict, "volume name is already in use")
	}
	if values[volumeEvidenceEnvironment] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	if values[volumeEvidenceEnvironment].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[volumeEvidenceProject] == nil {
		return errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if values[volumeEvidenceProject].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	for _, value := range values[volumeEvidenceTargetDeletion:] {
		if value != nil {
			return errs.New(errs.KindResourceInUse, "volume hierarchy deletion is in progress")
		}
	}
	return stateConflict("volume", record.ID)
}
