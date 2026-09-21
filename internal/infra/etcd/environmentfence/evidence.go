package environmentfence

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentMutationFenceStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

type Owner struct {
	Kind        backupruntime.BackupOperationKind
	OperationID string
	TaskID      string
}

type environmentMutationFenceConditionKind uint8

const (
	environmentMutationFenceEnvironment environmentMutationFenceConditionKind = iota + 1
	environmentMutationFenceProject
	environmentMutationFenceTenant
	environmentMutationFenceEnvironmentTombstone
	environmentMutationFenceOwnedDeletionTombstone
	environmentMutationFenceProjectTombstone
	environmentMutationFenceTenantTombstone
	environmentMutationFenceEpoch
	environmentMutationFenceLock
)

type environmentMutationFenceCondition struct {
	key         string
	stableID    string
	modRevision int64
	kind        environmentMutationFenceConditionKind
}

// Evidence carries immutable compare metadata captured by LoadOrdinary or
// LoadOwned. Its private state cannot be replaced by transaction callers.
type Evidence struct {
	environmentID string
	readRevision  int64
	owner         *Owner
	conditions    []environmentMutationFenceCondition
}

func LoadOrdinary(
	ctx context.Context,
	store environmentMutationFenceStore,
	environmentID string,
	readRevision int64,
) (Evidence, error) {
	return loadEnvironmentMutationFence(ctx, store, environmentID, readRevision, nil)
}

func LoadOwned(
	ctx context.Context,
	store environmentMutationFenceStore,
	environmentID string,
	readRevision int64,
	owner Owner,
) (Evidence, error) {
	if err := validateEnvironmentMutationFenceOwner(owner); err != nil {
		return Evidence{}, err
	}
	return loadEnvironmentMutationFence(ctx, store, environmentID, readRevision, &owner)
}

func loadEnvironmentMutationFence(
	ctx context.Context,
	store environmentMutationFenceStore,
	environmentID string,
	readRevision int64,
	owner *Owner,
) (Evidence, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return Evidence{}, err
	}
	if store == nil {
		return Evidence{}, errs.New(
			errs.KindInternal,
			"environment mutation fence store is required",
		)
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return Evidence{}, errs.New(
			errs.KindValidationFailed,
			"environment id is invalid",
		)
	}
	if readRevision <= 0 {
		return Evidence{}, errs.New(
			errs.KindValidationFailed,
			"environment mutation fence revision must be positive",
		)
	}

	baseKeys := []string{
		hierarchyrecord.EnvironmentKey(environmentID),
		hierarchyrecord.EnvironmentMutationEpochKey(environmentID),
		hierarchyrecord.EnvironmentOperationLockKey(environmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environmentID),
	}
	base, err := readEnvironmentMutationFenceKeys(ctx, store, baseKeys, readRevision)
	if err != nil {
		return Evidence{}, err
	}
	defer etcdstore.ClearValues(base.Values)
	if base.Values[0] == nil {
		return Evidence{}, errs.New(
			errs.KindEnvironmentNotFound,
			"environment was not found",
		)
	}
	environment, err := hierarchyrecord.DecodeEnvironment(base.Values[0].Value)
	if err != nil || environment.ID != environmentID {
		return Evidence{}, errs.New(
			errs.KindInternal,
			"environment mutation fence environment is corrupt",
		)
	}
	deletionOwner := owner != nil && owner.Kind == backupruntime.BackupOperationDeletion
	if deletionOwner {
		if err := validateOwnedEnvironmentDeletionTombstone(
			base.Values[3],
			environmentID,
			base.Values[0].ModRevision,
			*owner,
		); err != nil {
			return Evidence{}, err
		}
	} else if base.Values[3] != nil {
		return Evidence{}, errs.New(
			errs.KindResourceInUse,
			"environment hierarchy deletion is in progress",
		)
	}
	epoch, err := decodeEnvironmentMutationFenceEpoch(base.Values[1], environmentID)
	if err != nil {
		return Evidence{}, err
	}
	lockRevision, err := validateEnvironmentMutationFenceLock(base.Values[2], environmentID, owner)
	if err != nil {
		return Evidence{}, err
	}

	projectKeys := []string{
		hierarchyrecord.ProjectKey(environment.ProjectID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), environment.ProjectID),
	}
	projectRead, err := readEnvironmentMutationFenceKeys(ctx, store, projectKeys, readRevision)
	if err != nil {
		return Evidence{}, err
	}
	defer etcdstore.ClearValues(projectRead.Values)
	if projectRead.Values[0] == nil {
		return Evidence{}, errs.New(
			errs.KindProjectNotFound,
			"project was not found",
		)
	}
	project, err := hierarchyrecord.DecodeProject(projectRead.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID {
		return Evidence{}, errs.New(
			errs.KindInternal,
			"environment mutation fence project is corrupt",
		)
	}
	if projectRead.Values[1] != nil {
		return Evidence{}, errs.New(
			errs.KindResourceInUse,
			"project hierarchy deletion is in progress",
		)
	}

	conditions := []environmentMutationFenceCondition{
		{
			key:         baseKeys[0],
			stableID:    environment.ID,
			modRevision: base.Values[0].ModRevision,
			kind:        environmentMutationFenceEnvironment,
		},
		{
			key:         projectKeys[0],
			stableID:    project.ID,
			modRevision: projectRead.Values[0].ModRevision,
			kind:        environmentMutationFenceProject,
		},
	}
	if project.Kind == hierarchyrecord.ProjectKindTenant {
		tenantKeys := []string{
			hierarchyrecord.TenantKey(project.TenantID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID),
		}
		tenantRead, readErr := readEnvironmentMutationFenceKeys(
			ctx,
			store,
			tenantKeys,
			readRevision,
		)
		if readErr != nil {
			return Evidence{}, readErr
		}
		defer etcdstore.ClearValues(tenantRead.Values)
		if tenantRead.Values[0] == nil {
			return Evidence{}, errs.New(
				errs.KindTenantNotFound,
				"tenant was not found",
			)
		}
		tenant, decodeErr := hierarchyrecord.DecodeTenant(tenantRead.Values[0].Value)
		if decodeErr != nil || tenant.ID != project.TenantID {
			return Evidence{}, errs.New(
				errs.KindInternal,
				"environment mutation fence tenant is corrupt",
			)
		}
		if tenantRead.Values[1] != nil {
			return Evidence{}, errs.New(
				errs.KindResourceInUse,
				"tenant hierarchy deletion is in progress",
			)
		}
		conditions = append(conditions, environmentMutationFenceCondition{
			key: tenantKeys[0], stableID: tenant.ID,
			modRevision: tenantRead.Values[0].ModRevision, kind: environmentMutationFenceTenant,
		})
	} else if project.Kind != hierarchyrecord.ProjectKindBacking || project.TenantID != "" {
		return Evidence{}, errs.New(
			errs.KindInternal,
			"environment mutation fence project ownership is corrupt",
		)
	}

	environmentTombstoneCondition := environmentMutationFenceCondition{
		key: baseKeys[3], kind: environmentMutationFenceEnvironmentTombstone,
	}
	if deletionOwner {
		environmentTombstoneCondition.stableID = owner.TaskID
		environmentTombstoneCondition.modRevision = base.Values[3].ModRevision
		environmentTombstoneCondition.kind = environmentMutationFenceOwnedDeletionTombstone
	}
	conditions = append(
		conditions,
		environmentTombstoneCondition,
		environmentMutationFenceCondition{
			key:  projectKeys[1],
			kind: environmentMutationFenceProjectTombstone,
		},
	)
	if project.Kind == hierarchyrecord.ProjectKindTenant {
		conditions = append(conditions, environmentMutationFenceCondition{
			key:  deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID),
			kind: environmentMutationFenceTenantTombstone,
		})
	}
	conditions = append(conditions,
		environmentMutationFenceCondition{
			key: baseKeys[1], stableID: environmentID,
			modRevision: epoch.ModRevision, kind: environmentMutationFenceEpoch,
		},
		environmentMutationFenceCondition{
			key: baseKeys[2], stableID: environmentID,
			modRevision: lockRevision, kind: environmentMutationFenceLock,
		},
	)
	if len(conditions)+1 > etcdstore.MaximumOperations {
		return Evidence{}, errs.New(
			errs.KindInternal,
			"environment mutation fence exceeds transaction limit",
		)
	}

	evidence := Evidence{
		environmentID: environmentID,
		readRevision:  readRevision,
		conditions:    conditions,
	}
	if owner != nil {
		owned := *owner
		evidence.owner = &owned
	}
	return evidence, nil
}

func readEnvironmentMutationFenceKeys(
	ctx context.Context,
	store environmentMutationFenceStore,
	keys []string,
	readRevision int64,
) (*etcdstore.GetManyResult, error) {
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: readRevision})
	if err != nil {
		return nil, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != len(keys) {
		return nil, errs.New(
			errs.KindInternal,
			"environment mutation fence fixed-revision read is incomplete",
		)
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			etcdstore.ClearValues(result.Values)
			return nil, errs.New(
				errs.KindInternal,
				"environment mutation fence fixed-revision read is corrupt",
			)
		}
	}
	return result, nil
}

func decodeEnvironmentMutationFenceEpoch(value *etcdstore.KeyValue, environmentID string) (*etcdstore.KeyValue, error) {
	if value == nil {
		return nil, errs.New(errs.KindInternal, "environment mutation epoch is missing")
	}
	record, err := backupruntime.DecodeEnvironmentMutationEpochRecord(value.Value)
	if err != nil || record.EnvironmentID != environmentID {
		return nil, errs.New(errs.KindInternal, "environment mutation epoch is corrupt")
	}
	return value, nil
}

func validateEnvironmentMutationFenceLock(
	value *etcdstore.KeyValue,
	environmentID string,
	owner *Owner,
) (int64, error) {
	if value == nil {
		if owner != nil {
			return 0, errs.New(
				errs.KindStateConflict,
				"environment persistence operation ownership changed",
			)
		}
		return 0, nil
	}
	lock, err := backupruntime.DecodeBackupOperationLockRecord(value.Value)
	if err != nil || lock.EnvironmentID != environmentID {
		return 0, errs.New(errs.KindInternal, "environment operation lock is corrupt")
	}
	if owner == nil {
		return 0, errs.New(
			errs.KindResourceInUse,
			"environment persistence operation is in progress",
		)
	}
	if lock.Kind != owner.Kind || lock.OperationID != owner.OperationID ||
		lock.TaskID != owner.TaskID {
		return 0, errs.New(
			errs.KindStateConflict,
			"environment persistence operation ownership changed",
		)
	}
	return value.ModRevision, nil
}

func validateOwnedEnvironmentDeletionTombstone(
	value *etcdstore.KeyValue,
	environmentID string,
	environmentRevision int64,
	owner Owner,
) error {
	if value == nil {
		return errs.New(errs.KindStateConflict, "environment deletion tombstone is missing")
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(value.Value)
	if err != nil {
		return errs.New(errs.KindInternal, "environment deletion tombstone is corrupt")
	}
	if tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment || tombstone.TargetID != environmentID ||
		tombstone.TargetRevision != environmentRevision || tombstone.TaskID != owner.TaskID ||
		(tombstone.Phase != deletionrecord.DeletionPhaseHostEffects && tombstone.Phase != deletionrecord.DeletionPhaseFinalizing) {
		return errs.New(errs.KindStateConflict, "environment deletion tombstone ownership changed")
	}
	return nil
}

func validateEnvironmentMutationFenceOwner(owner Owner) error {
	switch owner.Kind {
	case backupruntime.BackupOperationBackup,
		backupruntime.BackupOperationRestore,
		backupruntime.BackupOperationRotation,
		backupruntime.BackupOperationPrune,
		backupruntime.BackupOperationDeletion:
	default:
		return errs.New(
			errs.KindValidationFailed,
			"environment mutation fence operation kind is invalid",
		)
	}
	if ids.Validate(ids.KindOperation, owner.OperationID) != nil ||
		ids.Validate(ids.KindTask, owner.TaskID) != nil {
		return errs.New(errs.KindValidationFailed, "environment mutation fence owner is invalid")
	}
	return nil
}

func (evidence Evidence) ReadRevision() int64 {
	return evidence.readRevision
}

func (evidence Evidence) EnvironmentID() string {
	return evidence.environmentID
}

func (evidence Evidence) ConditionCount() int {
	return len(evidence.conditions)
}

func (evidence Evidence) EpochRevision() (int64, bool) {
	for _, condition := range evidence.conditions {
		if condition.kind == environmentMutationFenceEpoch {
			return condition.modRevision, true
		}
	}
	return 0, false
}

func (evidence Evidence) TransactionConditions() []etcdstore.Condition {
	conditions := make([]etcdstore.Condition, len(evidence.conditions))
	for index, condition := range evidence.conditions {
		conditions[index] = etcdstore.Condition{Key: condition.key, ModRevision: condition.modRevision}
	}
	return conditions
}

func (evidence Evidence) EpochRewriteMutation() (etcdstore.Mutation, error) {
	value, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: evidence.environmentID,
	})
	if err != nil {
		return etcdstore.Mutation{}, err
	}
	return etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(evidence.environmentID), Value: value,
	}, nil
}

func (evidence Evidence) ClassifyConflict(values []*etcdstore.KeyValue) error {
	if len(values) != len(evidence.conditions) {
		return errs.New(
			errs.KindInternal,
			"environment mutation fence compare evidence is incomplete",
		)
	}
	for index, condition := range evidence.conditions {
		value := values[index]
		if value != nil && value.Key != condition.key {
			return errs.New(
				errs.KindInternal,
				"environment mutation fence compare evidence is corrupt",
			)
		}
		switch condition.kind {
		case environmentMutationFenceEnvironment:
			if value == nil {
				return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
			}
			record, err := hierarchyrecord.DecodeEnvironment(value.Value)
			if err != nil || record.ID != condition.stableID {
				return errs.New(
					errs.KindInternal,
					"environment mutation fence environment is corrupt",
				)
			}
			if value.ModRevision != condition.modRevision {
				return recordcodec.StateConflict("environment", condition.stableID)
			}
		case environmentMutationFenceProject:
			if value == nil {
				return errs.New(errs.KindProjectNotFound, "project was not found")
			}
			record, err := hierarchyrecord.DecodeProject(value.Value)
			if err != nil || record.ID != condition.stableID {
				return errs.New(errs.KindInternal, "environment mutation fence project is corrupt")
			}
			if value.ModRevision != condition.modRevision {
				return recordcodec.StateConflict("project", condition.stableID)
			}
		case environmentMutationFenceTenant:
			if value == nil {
				return errs.New(errs.KindTenantNotFound, "tenant was not found")
			}
			record, err := hierarchyrecord.DecodeTenant(value.Value)
			if err != nil || record.ID != condition.stableID {
				return errs.New(errs.KindInternal, "environment mutation fence tenant is corrupt")
			}
			if value.ModRevision != condition.modRevision {
				return recordcodec.StateConflict("tenant", condition.stableID)
			}
		case environmentMutationFenceEnvironmentTombstone,
			environmentMutationFenceProjectTombstone,
			environmentMutationFenceTenantTombstone:
			if value != nil {
				return errs.New(
					errs.KindResourceInUse,
					"environment hierarchy deletion is in progress",
				)
			}
		case environmentMutationFenceOwnedDeletionTombstone:
			if evidence.owner == nil {
				return errs.New(
					errs.KindInternal,
					"environment deletion tombstone owner is missing",
				)
			}
			if err := validateOwnedEnvironmentDeletionTombstone(
				value,
				evidence.environmentID,
				evidence.conditions[0].modRevision,
				*evidence.owner,
			); err != nil {
				return err
			}
			if value.ModRevision != condition.modRevision {
				return recordcodec.StateConflict("environment deletion tombstone", condition.stableID)
			}
		case environmentMutationFenceEpoch:
			if _, err := decodeEnvironmentMutationFenceEpoch(
				value,
				condition.stableID,
			); err != nil {
				return err
			}
			if value.ModRevision != condition.modRevision {
				return recordcodec.StateConflict("environment mutation epoch", condition.stableID)
			}
		case environmentMutationFenceLock:
			if evidence.owner == nil {
				if value == nil {
					continue
				}
				if _, err := validateEnvironmentMutationFenceLock(
					value,
					condition.stableID,
					evidence.owner,
				); err != nil {
					kind, ok := errs.KindOf(err)
					if !ok || kind != errs.KindResourceInUse {
						return err
					}
				}
				return recordcodec.StateConflict("environment operation lock", condition.stableID)
			}
			if value == nil {
				return recordcodec.StateConflict("environment operation lock", condition.stableID)
			}
			if _, err := validateEnvironmentMutationFenceLock(
				value,
				condition.stableID,
				evidence.owner,
			); err != nil {
				return err
			}
			if value.ModRevision != condition.modRevision {
				return recordcodec.StateConflict("environment operation lock", condition.stableID)
			}
		default:
			return errs.New(errs.KindInternal, "environment mutation fence compare kind is invalid")
		}
	}
	return nil
}
