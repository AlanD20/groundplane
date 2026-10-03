package hierarchydeletionplanning

import (
	"context"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Planner) freezeEnvironmentBackupMembership(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	environmentID string,
	prerequisites []string,
) ([]HierarchyDeletionMembershipNode, error) {
	nodes, err := repository.freezeEnvironmentBackupRemoteMembership(ctx, operation, environmentID, prerequisites)
	if err != nil {
		return nil, err
	}
	remoteTerminals := terminalHierarchyDeletionNodes(nodes)
	if len(remoteTerminals) == 0 {
		remoteTerminals = append([]string(nil), prerequisites...)
	}
	for _, descriptor := range []struct {
		prefix     string
		targetKind hierarchydeletion.HierarchyDeletionActionTargetKind
		action     hierarchydeletion.HierarchyDeletionActionKind
		finalizer  string
	}{
		{backupruntime.BackupScheduleCursorPrefix + environmentID + "/", "backup-schedule", hierarchydeletion.HierarchyDeletionBackupScheduleFinalize, "backup-schedule.finalize"},
		{backupruntime.BackupDueOutcomePrefix + environmentID + "/", "backup-due", hierarchydeletion.HierarchyDeletionBackupDueFinalize, "backup-due.finalize"},
	} {
		part, freezeErr := repository.freezeBackupExactPrefix(ctx, operation, descriptor.prefix,
			descriptor.targetKind, descriptor.action, descriptor.finalizer, remoteTerminals)
		if freezeErr != nil {
			return nil, freezeErr
		}
		nodes = append(nodes, part...)
	}

	historyDescriptors := []hierarchyDeletionIndexedResource{
		{targetKind: "backup-run", actionKind: hierarchydeletion.HierarchyDeletionBackupRunDetach,
			ownerPrefix: func(owner string) string { return backupruntime.BackupRunEnvironmentPrefix + owner + "/" },
			primaryKey:  backupruntime.BackupRunKey, stableIDKind: ids.KindTask,
			validateOwner: validateHierarchyDeletionBackupRunOwner, controller: true},
		{targetKind: "backup-restore", actionKind: hierarchydeletion.HierarchyDeletionBackupRestoreDetach,
			ownerPrefix: func(owner string) string { return backupruntime.BackupRestoreEnvironmentPrefix + owner + "/" },
			primaryKey:  backupruntime.BackupRestoreKey, stableIDKind: ids.KindTask,
			validateOwner: validateHierarchyDeletionBackupRestoreOwner, controller: true},
		{targetKind: "backup-key-rotation", actionKind: hierarchydeletion.HierarchyDeletionBackupKeyRotationDetach,
			ownerPrefix: func(owner string) string { return backupruntime.BackupKeyRotationEnvironmentPrefix + owner + "/" },
			primaryKey:  backupruntime.BackupKeyRotationKey, stableIDKind: ids.KindTask,
			validateOwner: validateHierarchyDeletionBackupRotationOwner, controller: true},
	}
	for _, descriptor := range historyDescriptors {
		part, freezeErr := repository.freezeIndexedResource(ctx, operation, environmentID, descriptor)
		if freezeErr != nil {
			return nil, freezeErr
		}
		for index := range part {
			part[index].PrerequisiteNodeIDs = append([]string(nil), remoteTerminals...)
		}
		nodes = append(nodes, part...)
	}

	sources, err := repository.freezeIndexedResource(ctx, operation, environmentID, hierarchyDeletionIndexedResource{
		targetKind: "backup-source", actionKind: hierarchydeletion.HierarchyDeletionBackupSourceFinalize,
		ownerPrefix: backuppolicy.BackupSourceEnvironmentPrefix, primaryKey: backuppolicy.BackupSourceKey,
		stableIDKind: ids.KindBackupSource, validateOwner: validateHierarchyDeletionBackupSourceOwner, controller: true,
	})
	if err != nil {
		return nil, err
	}
	for _, source := range sources {
		retention, freezeErr := repository.freezeBackupExactPrefix(ctx, operation,
			backupruntime.BackupRetentionPrefix+source.TargetID+"/", "backup-retention",
			hierarchydeletion.HierarchyDeletionBackupRetentionFinalize, "backup-retention.finalize", remoteTerminals)
		if freezeErr != nil {
			return nil, freezeErr
		}
		nodes = append(nodes, retention...)
	}
	localTerminals := terminalHierarchyDeletionNodes(nodes)
	for index := range sources {
		sources[index].PrerequisiteNodeIDs = append([]string(nil), localTerminals...)
	}
	nodes = append(nodes, sources...)

	ownershipPrerequisites := terminalHierarchyDeletionNodes(nodes)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		backuppolicy.BackupPolicyKey(environmentID), backuppolicy.BackupKeyKey(environmentID),
		backuppolicy.BackupKeyValueKey(environmentID),
	}, Revision: operation.SnapshotRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != operation.SnapshotRevision || len(read.Values) != 3 {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if policyValue := read.Values[0]; policyValue != nil {
		policy, decodeErr := backuppolicy.DecodeBackupPolicyRecord(policyValue.Value)
		if decodeErr != nil || policy.EnvironmentID != environmentID {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		node := hierarchyDeletionControllerNode(
			"backup-policy:"+environmentID+":finalize", "backup-policy", environmentID,
			hierarchydeletion.HierarchyDeletionBackupPolicyFinalize, policyValue.ModRevision,
			ownershipPrerequisites, "backup-policy.finalize",
			hierarchydeletion.HierarchyDeletionBytesDigest(policyValue.Value),
		)
		nodes = append(nodes, node)
		ownershipPrerequisites = []string{node.NodeID}
	}
	if (read.Values[1] == nil) != (read.Values[2] == nil) {
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if read.Values[1] != nil {
		metadata, metadataErr := backuppolicy.DecodeBackupKeyRecord(read.Values[1].Value)
		encrypted, encryptedErr := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[2].Value)
		if metadataErr != nil || encryptedErr != nil || metadata.EnvironmentID != environmentID ||
			encrypted.EnvironmentID != environmentID || metadata.KeyEra != encrypted.KeyEra {
			clear(encrypted.Ciphertext)
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		clear(encrypted.Ciphertext)
		nodes = append(nodes, hierarchyDeletionControllerNode(
			"key-material:"+environmentID+":remove", "key-material", environmentID,
			hierarchydeletion.HierarchyDeletionKeyMaterialRemove, read.Values[1].ModRevision,
			ownershipPrerequisites, "key-material.remove",
			hierarchydeletion.HierarchyDeletionBackupKeyDigest(read.Values[1].Value, read.Values[2].Value),
		))
	}
	return nodes, nil
}

func (repository *Planner) freezeBackupExactPrefix(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	prefix string,
	targetKind hierarchydeletion.HierarchyDeletionActionTargetKind,
	action hierarchydeletion.HierarchyDeletionActionKind,
	finalizer string,
	prerequisites []string,
) ([]HierarchyDeletionMembershipNode, error) {
	nodes := make([]HierarchyDeletionMembershipNode, 0)
	start := ""
	for {
		page, err := repository.store.Range(ctx, keyvalue.RangeRequest{Prefix: prefix,
			StartExclusive: start, Limit: 128, Revision: operation.SnapshotRevision})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != operation.SnapshotRevision || len(page.Values) > 128 {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for _, item := range page.Values {
			if item.Key == prefix || !strings.HasPrefix(item.Key, prefix) || item.ModRevision <= 0 {
				keyvalue.ClearRangeValues(page.Values)
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			digest := hierarchydeletion.HierarchyDeletionBytesDigest(item.Value)
			keyDigest := hierarchydeletion.HierarchyDeletionFoldDigest(
				"groundplane-deletion-backup-exact-key-v1", item.Key,
			)
			nodes = append(nodes, hierarchyDeletionControllerNode(
				fmt.Sprintf("%s:%s:%s", targetKind, keyDigest, action), targetKind, item.Key,
				action, item.ModRevision, prerequisites, finalizer, digest,
			))
			start = item.Key
		}
		more := page.More
		keyvalue.ClearRangeValues(page.Values)
		if !more {
			return nodes, nil
		}
		if start == "" {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
}

func validateHierarchyDeletionBackupSourceOwner(value []byte, id, owner string) error {
	record, err := backuppolicy.DecodeBackupSourceRecord(value)
	if err != nil || record.ID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	return nil
}

func validateHierarchyDeletionBackupRunOwner(value []byte, id, owner string) error {
	record, err := backupruntime.DecodeBackupRunRecord(value)
	if err != nil || record.TaskID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if !backupruntime.TerminalBackupRunState(record.State) {
		return errs.New(errs.KindResourceInUse, "backup run is not terminal")
	}
	return nil
}

func validateHierarchyDeletionBackupRestoreOwner(value []byte, id, owner string) error {
	record, err := backupruntime.DecodeBackupRestoreRecord(value)
	if err != nil || record.TaskID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if record.State != backupruntime.BackupRestoreCompleted && record.State != backupruntime.BackupRestoreFailedSafe {
		return errs.New(errs.KindResourceInUse, "backup restore is not safely terminal")
	}
	return nil
}

func validateHierarchyDeletionBackupRotationOwner(value []byte, id, owner string) error {
	record, err := backupruntime.DecodeBackupKeyRotationRecord(value)
	if err != nil || record.TaskID != id || record.EnvironmentID != owner {
		return hierarchydeletion.CorruptHierarchyDeletion()
	}
	if record.State != backupruntime.BackupKeyRotationApplied {
		return errs.New(errs.KindResourceInUse, "backup key rotation is not safely applied")
	}
	return nil
}
