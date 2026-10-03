package hierarchydeletionplanning

import (
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Planner) freezeEnvironmentBackupRemoteMembership(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	environmentID string,
	prerequisites []string,
) ([]HierarchyDeletionMembershipNode, error) {
	points, err := repository.freezeEnvironmentRecoveryPoints(ctx, operation, environmentID, prerequisites)
	if err != nil {
		return nil, err
	}
	orphans, err := repository.freezeEnvironmentBackupOrphans(ctx, operation, environmentID, prerequisites)
	if err != nil {
		return nil, err
	}
	return append(points, orphans...), nil
}

func (repository *Planner) freezeEnvironmentRecoveryPoints(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	environmentID string,
	prerequisites []string,
) ([]HierarchyDeletionMembershipNode, error) {
	prefix := backupruntime.BackupRecoveryPointEnvironmentPrefix + environmentID + "/"
	return repository.freezeBackupRemoteIndex(
		ctx,
		operation,
		prefix,
		func(item keyvalue.KeyValue) (HierarchyDeletionMembershipNode, error) {
			pointID := string(item.Value)
			expected, err := backupruntime.BackupRecoveryPointEnvironmentIndexKey(environmentID, pointID)
			if err != nil || expected != item.Key {
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			first, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
				backupruntime.BackupRecoveryPointPruneKey(pointID), backupruntime.BackupRecoveryPointKey(pointID),
			}, Revision: operation.SnapshotRevision})
			if err != nil {
				return HierarchyDeletionMembershipNode{}, err
			}
			if first == nil || first.ReadRevision != operation.SnapshotRevision || len(first.Values) != 2 ||
				first.Values[0] != nil || first.Values[1] == nil {
				if first != nil {
					keyvalue.ClearValues(first.Values)
				}
				return HierarchyDeletionMembershipNode{}, errs.New(errs.KindStateConflict,
					"backup recovery point is already owned by retention pruning")
			}
			record, err := backupruntime.DecodeBackupRecoveryPointRecord(first.Values[1].Value)
			if err != nil || record.ID != pointID || record.EnvironmentID != environmentID {
				keyvalue.ClearValues(first.Values)
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			keys, err := backupruntime.BackupPruneAuthorityKeys(record.BackupRecoveryPointSnapshot)
			if err != nil {
				keyvalue.ClearValues(first.Values)
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			authority, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys,
				Revision: operation.SnapshotRevision})
			if err != nil {
				keyvalue.ClearValues(first.Values)
				return HierarchyDeletionMembershipNode{}, err
			}
			if authority == nil || authority.ReadRevision != operation.SnapshotRevision || len(authority.Values) != 5 ||
				authority.Values[0] != nil || authority.Values[1] == nil || authority.Values[2] == nil ||
				authority.Values[3] == nil || authority.Values[4] == nil ||
				authority.Values[1].ModRevision != first.Values[1].ModRevision ||
				authority.Values[2].Key != item.Key || authority.Values[2].ModRevision != item.ModRevision ||
				string(authority.Values[2].Value) != pointID || string(authority.Values[3].Value) != pointID ||
				string(authority.Values[4].Value) != pointID {
				keyvalue.ClearValues(first.Values)
				if authority != nil {
					keyvalue.ClearValues(authority.Values)
				}
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			digest := hierarchydeletion.HierarchyDeletionBytesDigest(first.Values[1].Value)
			revision := first.Values[1].ModRevision
			keyvalue.ClearValues(first.Values)
			keyvalue.ClearValues(authority.Values)
			return hierarchyDeletionControllerNode(
				fmt.Sprintf("recovery-point:%s:remove", pointID), "recovery-point", pointID,
				hierarchydeletion.HierarchyDeletionRecoveryPointRemove, revision, prerequisites,
				"recovery-point.remove", digest,
			), nil
		},
	)
}

func (repository *Planner) freezeEnvironmentBackupOrphans(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	environmentID string,
	prerequisites []string,
) ([]HierarchyDeletionMembershipNode, error) {
	prefix := backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/"
	return repository.freezeBackupRemoteIndex(
		ctx,
		operation,
		prefix,
		func(item keyvalue.KeyValue) (HierarchyDeletionMembershipNode, error) {
			pointID := string(item.Value)
			expected, err := backupruntime.BackupOrphanEnvironmentIndexKey(environmentID, pointID)
			if err != nil || expected != item.Key {
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			primaryKey := backupruntime.BackupOrphanKey(pointID)
			primary, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{primaryKey},
				Revision: operation.SnapshotRevision})
			if err != nil {
				return HierarchyDeletionMembershipNode{}, err
			}
			if primary == nil || primary.ReadRevision != operation.SnapshotRevision || len(primary.Values) != 1 ||
				primary.Values[0] == nil {
				if primary != nil {
					keyvalue.ClearValues(primary.Values)
				}
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			record, err := backupruntime.DecodeBackupOrphanRecord(primary.Values[0].Value)
			if err != nil || record.Target.ID != pointID || record.Target.EnvironmentID != environmentID {
				keyvalue.ClearValues(primary.Values)
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			if record.CleanupProof == (backupruntime.BackupOrphanCleanupProof{}) {
				keyvalue.ClearValues(primary.Values)
				return HierarchyDeletionMembershipNode{}, errs.New(errs.KindStateConflict,
					"backup orphan staging cleanup is not proved")
			}
			connectorKey, err := backupruntime.BackupOrphanConnectorIndexKey(record.Target.ConnectorID, pointID)
			if err != nil {
				keyvalue.ClearValues(primary.Values)
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			authority, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
				primaryKey, item.Key, connectorKey,
			}, Revision: operation.SnapshotRevision})
			if err != nil {
				keyvalue.ClearValues(primary.Values)
				return HierarchyDeletionMembershipNode{}, err
			}
			if authority == nil || authority.ReadRevision != operation.SnapshotRevision || len(authority.Values) != 3 ||
				backupruntime.ValidateBackupOrphanCompanionEvidence(authority.Values, record) != nil ||
				authority.Values[0].ModRevision != primary.Values[0].ModRevision ||
				authority.Values[1].ModRevision != item.ModRevision {
				keyvalue.ClearValues(primary.Values)
				if authority != nil {
					keyvalue.ClearValues(authority.Values)
				}
				return HierarchyDeletionMembershipNode{}, hierarchydeletion.CorruptHierarchyDeletion()
			}
			digest := hierarchydeletion.HierarchyDeletionBytesDigest(primary.Values[0].Value)
			revision := primary.Values[0].ModRevision
			keyvalue.ClearValues(primary.Values)
			keyvalue.ClearValues(authority.Values)
			return hierarchyDeletionControllerNode(
				fmt.Sprintf("orphan-object:%s:remove", pointID), "orphan-object", pointID,
				hierarchydeletion.HierarchyDeletionOrphanObjectRemove, revision, prerequisites,
				"orphan-object.remove", digest,
			), nil
		},
	)
}

func (repository *Planner) freezeBackupRemoteIndex(ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	prefix string,
	freeze func(keyvalue.KeyValue) (HierarchyDeletionMembershipNode, error),
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
			node, freezeErr := freeze(item)
			if freezeErr != nil {
				keyvalue.ClearRangeValues(page.Values)
				return nil, freezeErr
			}
			nodes = append(nodes, node)
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
