package etcd

import (
	"context"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func (repository *HierarchyDeletionRepository) freezeIndexedResource(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	ownerID string,
	descriptor hierarchyDeletionIndexedResource,
) ([]HierarchyDeletionMembershipNode, error) {
	targets, err := repository.hierarchyDeletionIndexedTargets(
		ctx, operation.Tombstone.SnapshotRevision, descriptor.ownerPrefix(ownerID), descriptor.primaryKey,
		descriptor.stableIDKind, descriptor.validateOwner, ownerID,
	)
	if err != nil {
		return nil, err
	}
	nodes := make([]HierarchyDeletionMembershipNode, 0, len(targets))
	for _, target := range targets {
		nodeID := fmt.Sprintf("%s:%s:%s", descriptor.targetKind, target.id, descriptor.actionKind)
		if descriptor.controller {
			nodes = append(nodes, hierarchyDeletionControllerNode(
				nodeID, descriptor.targetKind, target.id, descriptor.actionKind,
				target.revision, nil, hierarchyDeletionControllerFinalizer(descriptor.actionKind), target.digest,
			))
		} else {
			node := hierarchyDeletionAgentNode(
				nodeID, descriptor.targetKind, target.id, descriptor.actionKind,
				target.revision, nil, operation.Tombstone.OperationID,
			)
			node.fixedInputDigest = target.digest
			nodes = append(nodes, node)
		}
	}
	return nodes, nil
}

type hierarchyDeletionIndexedTarget struct {
	id       string
	revision int64
	digest   string
}

func (repository *HierarchyDeletionRepository) hierarchyDeletionIndexedTargets(
	ctx context.Context,
	revision int64,
	prefix string,
	primaryKey func(string) string,
	kind ids.Kind,
	validateOwner func([]byte, string, string) error,
	ownerID string,
) ([]hierarchyDeletionIndexedTarget, error) {
	if revision <= 0 || prefix == "" || primaryKey == nil || validateOwner == nil {
		return nil, errs.New(errs.KindInternal, "hierarchy deletion membership descriptor is invalid")
	}
	idsAtRevision := make([]string, 0)
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 128, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for _, value := range page.Values {
			if validateListKey(prefix, value.Key, kind) != nil {
				clearRangeValues(page.Values)
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			id := strings.TrimPrefix(value.Key, prefix)
			if string(value.Value) != id {
				clearRangeValues(page.Values)
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			idsAtRevision = append(idsAtRevision, id)
			start = value.Key
		}
		more := page.More
		clearRangeValues(page.Values)
		if !more {
			break
		}
		if start == "" {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	targets := make([]hierarchyDeletionIndexedTarget, 0, len(idsAtRevision))
	for begin := 0; begin < len(idsAtRevision); begin += 32 {
		end := min(begin+32, len(idsAtRevision))
		keys := make([]string, end-begin)
		for index, id := range idsAtRevision[begin:end] {
			keys[index] = primaryKey(id)
		}
		read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for index, value := range read.Values {
			id := idsAtRevision[begin+index]
			if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
				etcdstore.ClearValues(read.Values)
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			if validateErr := validateOwner(value.Value, id, ownerID); validateErr != nil {
				etcdstore.ClearValues(read.Values)
				return nil, validateErr
			}
			targets = append(targets, hierarchyDeletionIndexedTarget{
				id: id, revision: value.ModRevision, digest: hierarchyDeletionBytesDigest(value.Value),
			})
		}
		etcdstore.ClearValues(read.Values)
	}
	return targets, nil
}
