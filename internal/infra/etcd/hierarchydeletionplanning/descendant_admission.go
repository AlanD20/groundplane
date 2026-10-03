package hierarchydeletionplanning

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Planner) RequireDescendantsAvailable(
	ctx context.Context,
	revision int64,
	targetKind hierarchydeletion.HierarchyDeletionTargetKind,
	targetID string,
) error {
	switch targetKind {
	case hierarchydeletion.HierarchyDeletionTargetTenant:
		projects, err := repository.hierarchyDeletionIndexedTargets(
			ctx,
			revision,
			hierarchyrecord.ProjectTenantOwnerPrefix(targetID),
			hierarchyrecord.ProjectKey,
			ids.KindProject,
			func(value []byte, id, owner string) error {
				record, decodeErr := hierarchyrecord.DecodeProject(value)
				if decodeErr != nil || record.ID != id || record.TenantID != owner ||
					record.Kind != hierarchyrecord.ProjectKindTenant {
					return hierarchydeletion.CorruptHierarchyDeletion()
				}
				if record.DeletionTaskID != "" {
					return hierarchyDeletionDescendantUnavailable()
				}
				return nil
			},
			targetID,
		)
		if err != nil {
			return err
		}
		for _, project := range projects {
			if err := repository.requireProjectDeletionDescendantsAvailable(ctx, revision, project.id); err != nil {
				return err
			}
		}
		return nil
	case hierarchydeletion.HierarchyDeletionTargetProject, hierarchydeletion.HierarchyDeletionTargetBacking:
		return repository.requireProjectDeletionDescendantsAvailable(ctx, revision, targetID)
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		return repository.requireEnvironmentBackupDeletionAvailable(ctx, revision, targetID)
	default:
		return errs.New(errs.KindValidationFailed, "hierarchy deletion target kind is invalid")
	}
}

func (repository *Planner) requireProjectDeletionDescendantsAvailable(
	ctx context.Context,
	revision int64,
	projectID string,
) error {
	environments, err := repository.hierarchyDeletionIndexedTargets(
		ctx,
		revision,
		hierarchyrecord.EnvironmentOwnerPrefix(projectID),
		hierarchyrecord.EnvironmentKey,
		ids.KindEnvironment,
		func(value []byte, id, owner string) error {
			record, decodeErr := hierarchyrecord.DecodeEnvironment(value)
			if decodeErr != nil || record.ID != id || record.ProjectID != owner {
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			if record.DeletionTaskID != "" {
				return hierarchyDeletionDescendantUnavailable()
			}
			return nil
		},
		projectID,
	)
	if err != nil {
		return err
	}
	for _, environment := range environments {
		if err := repository.requireEnvironmentBackupDeletionAvailable(ctx, revision, environment.id); err != nil {
			return err
		}
	}
	return nil
}

func (repository *Planner) requireEnvironmentBackupDeletionAvailable(
	ctx context.Context,
	revision int64,
	environmentID string,
) error {
	if err := repository.requireEnvironmentBackupHistoryAvailable(ctx, revision, environmentID); err != nil {
		return err
	}
	if err := repository.requireEnvironmentRecoveryPointPruneAbsent(ctx, revision, environmentID); err != nil {
		return err
	}
	prefix := backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/"
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 128, Revision: revision,
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 128 {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		for _, item := range page.Values {
			pointID := string(item.Value)
			expected, keyErr := backupruntime.BackupOrphanEnvironmentIndexKey(environmentID, pointID)
			if keyErr != nil || expected != item.Key {
				etcdstore.ClearRangeValues(page.Values)
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			primary, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{backupruntime.BackupOrphanKey(pointID)}, Revision: revision,
			})
			if readErr != nil {
				etcdstore.ClearRangeValues(page.Values)
				return readErr
			}
			if primary == nil || primary.ReadRevision != revision || len(primary.Values) != 1 ||
				primary.Values[0] == nil {
				if primary != nil {
					etcdstore.ClearValues(primary.Values)
				}
				etcdstore.ClearRangeValues(page.Values)
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			record, decodeErr := backupruntime.DecodeBackupOrphanRecord(primary.Values[0].Value)
			missingProof := decodeErr == nil && record.CleanupProof == (backupruntime.BackupOrphanCleanupProof{})
			invalid := decodeErr != nil || record.Target.ID != pointID || record.Target.EnvironmentID != environmentID
			etcdstore.ClearValues(primary.Values)
			if invalid {
				etcdstore.ClearRangeValues(page.Values)
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			if missingProof {
				etcdstore.ClearRangeValues(page.Values)
				return errs.New(errs.KindResourceInUse, "backup orphan staging cleanup is not proved")
			}
			start = item.Key
		}
		more := page.More
		etcdstore.ClearRangeValues(page.Values)
		if !more {
			return nil
		}
		if start == "" {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
}

func (repository *Planner) requireEnvironmentBackupHistoryAvailable(
	ctx context.Context,
	revision int64,
	environmentID string,
) error {
	descriptors := []hierarchyDeletionIndexedResource{
		{ownerPrefix: func(owner string) string { return backupruntime.BackupRunEnvironmentPrefix + owner + "/" },
			primaryKey: backupruntime.BackupRunKey, stableIDKind: ids.KindTask,
			validateOwner: validateHierarchyDeletionBackupRunOwner},
		{ownerPrefix: func(owner string) string { return backupruntime.BackupRestoreEnvironmentPrefix + owner + "/" },
			primaryKey: backupruntime.BackupRestoreKey, stableIDKind: ids.KindTask,
			validateOwner: validateHierarchyDeletionBackupRestoreOwner},
		{
			ownerPrefix:   func(owner string) string { return backupruntime.BackupKeyRotationEnvironmentPrefix + owner + "/" },
			primaryKey:    backupruntime.BackupKeyRotationKey,
			stableIDKind:  ids.KindTask,
			validateOwner: validateHierarchyDeletionBackupRotationOwner,
		},
	}
	for _, descriptor := range descriptors {
		if _, err := repository.hierarchyDeletionIndexedTargets(
			ctx, revision, descriptor.ownerPrefix(environmentID), descriptor.primaryKey,
			descriptor.stableIDKind, descriptor.validateOwner, environmentID,
		); err != nil {
			return err
		}
	}
	return nil
}

func (repository *Planner) requireEnvironmentRecoveryPointPruneAbsent(
	ctx context.Context,
	revision int64,
	environmentID string,
) error {
	prefix := backupruntime.BackupRecoveryPointEnvironmentPrefix + environmentID + "/"
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 128, Revision: revision,
		})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 128 {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
		keys := make([]string, len(page.Values))
		for index, item := range page.Values {
			pointID := string(item.Value)
			expected, keyErr := backupruntime.BackupRecoveryPointEnvironmentIndexKey(environmentID, pointID)
			if keyErr != nil || expected != item.Key {
				etcdstore.ClearRangeValues(page.Values)
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			keys[index] = backupruntime.BackupRecoveryPointPruneKey(pointID)
			start = item.Key
		}
		if len(keys) != 0 {
			read, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
			if readErr != nil {
				etcdstore.ClearRangeValues(page.Values)
				return readErr
			}
			if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
				etcdstore.ClearRangeValues(page.Values)
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			for _, value := range read.Values {
				if value != nil {
					etcdstore.ClearValues(read.Values)
					etcdstore.ClearRangeValues(page.Values)
					return errs.New(errs.KindResourceInUse, "backup recovery point pruning is already in progress")
				}
			}
			etcdstore.ClearValues(read.Values)
		}
		more := page.More
		etcdstore.ClearRangeValues(page.Values)
		if !more {
			return nil
		}
		if start == "" {
			return hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
}

func hierarchyDeletionDescendantUnavailable() error {
	return errs.New(errs.KindResourceInUse, "descendant hierarchy deletion is already in progress")
}
