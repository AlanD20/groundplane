package etcd

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
)

func (repository *HierarchyDeletionRepository) freezeEnvironmentMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	environmentID string,
	environmentRevision int64,
	environmentDigest string,
) ([]HierarchyDeletionMembershipNode, error) {
	activeScripts, err := readActiveScriptSet(
		ctx,
		repository.store,
		environmentID,
		operation.Tombstone.SnapshotRevision,
	)
	if err != nil {
		return nil, err
	}
	projection, found, err := currentEnvironmentProjectionAtRevision(
		ctx, repository.store, environmentID, operation.Tombstone.SnapshotRevision,
	)
	if err != nil {
		return nil, err
	}
	if found && (projection.ReadRevision != operation.Tombstone.SnapshotRevision ||
		projection.Record.EnvironmentID != environmentID) {
		return nil, corruptHierarchyDeletion()
	}
	descriptors := []hierarchyDeletionIndexedResource{
		{
			targetKind:    "attach",
			actionKind:    HierarchyDeletionAttachGrantRevoke,
			ownerPrefix:   attachrecord.AttachOwnerPrefix,
			primaryKey:    attachrecord.AttachKey,
			stableIDKind:  ids.KindAttach,
			validateOwner: validateHierarchyDeletionAttachOwner,
		},
		{
			targetKind:    "release-group",
			actionKind:    HierarchyDeletionReleaseGroupRemove,
			ownerPrefix:   func(owner string) string { return releaseGroupOwnerPrefix + owner + "/" },
			primaryKey:    releaseGroupRecordKey,
			stableIDKind:  ids.KindReleaseGroup,
			validateOwner: validateHierarchyDeletionReleaseGroupOwner,
			controller:    true,
		},
		{
			targetKind:    "entry",
			actionKind:    HierarchyDeletionEntryRemove,
			ownerPrefix:   entryOwnerCollectionPrefix,
			primaryKey:    entryrecord.RecordKey,
			stableIDKind:  ids.KindEnvEntry,
			validateOwner: validateHierarchyDeletionEntryOwner,
			controller:    true,
		},
		{
			targetKind:    "component",
			actionKind:    HierarchyDeletionComponentRemove,
			ownerPrefix:   componentrecord.EnvironmentOwnerPrefix,
			primaryKey:    componentrecord.RecordKey,
			stableIDKind:  ids.KindComponent,
			validateOwner: validateHierarchyDeletionComponentOwner,
			controller:    true,
		},
		{targetKind: "script", actionKind: HierarchyDeletionScriptRemove,
			ownerPrefix: func(owner string) string {
				return scriptrecord.ScriptSetOwnerPrefix(owner, activeScripts.Record.GenerationID)
			},
			primaryKey: func(id string) string {
				return scriptrecord.ScriptSetScriptKey(environmentID, activeScripts.Record.GenerationID, id)
			},
			stableIDKind: ids.KindScript, validateOwner: validateHierarchyDeletionScriptOwner, controller: true},
		{
			targetKind:    "connector",
			actionKind:    HierarchyDeletionConnectorFinalize,
			ownerPrefix:   connectorEnvironmentPrefix,
			primaryKey:    connectorrecord.RecordKey,
			stableIDKind:  ids.KindConnector,
			validateOwner: validateHierarchyDeletionConnectorOwner,
			controller:    true,
		},
	}
	cleanup := hierarchyDeletionAgentNode(
		"environment:"+environmentID+":cleanup", "environment", environmentID,
		HierarchyDeletionEnvironmentAgentCleanup, environmentRevision, nil, operation.Tombstone.OperationID,
	)
	cleanup.fixedInputDigest = environmentDigest
	nodes := []HierarchyDeletionMembershipNode{cleanup}
	services, err := repository.freezeEnvironmentServiceRuntimeMembership(ctx, operation, projection)
	if err != nil {
		return nil, err
	}
	for _, descriptor := range descriptors {
		part, err := repository.freezeIndexedResource(ctx, operation, environmentID, descriptor)
		if err != nil {
			return nil, err
		}
		prerequisites := terminalHierarchyDeletionNodes(nodes)
		for index := range part {
			part[index].PrerequisiteNodeIDs = append([]string(nil), prerequisites...)
		}
		if descriptor.actionKind == HierarchyDeletionAttachGrantRevoke {
			for _, grant := range part {
				nodes = append(nodes, grant)
				detach := hierarchyDeletionAgentNode(
					"attach:"+grant.TargetID+":detach",
					"attach",
					grant.TargetID,
					HierarchyDeletionAttachDetach,
					grant.TargetRevision,
					[]string{grant.NodeID},
					operation.Tombstone.OperationID,
				)
				detach.fixedInputDigest = grant.fixedInputDigest
				nodes = append(nodes, detach)
			}
			continue
		}
		nodes = append(nodes, part...)
		if descriptor.actionKind == HierarchyDeletionReleaseGroupRemove {
			prerequisites := terminalHierarchyDeletionNodes(nodes)
			for index := range services {
				services[index].PrerequisiteNodeIDs = append([]string(nil), prerequisites...)
			}
			nodes = append(nodes, services...)
		}
	}
	prerequisites := terminalHierarchyDeletionNodes(nodes)
	for _, desired := range projection.Record.DesiredZones {
		evidence, evidenceErr := newHierarchyDeletionZoneEvidence(projection, desired)
		if evidenceErr != nil {
			return nil, evidenceErr
		}
		value, encodeErr := encodeHierarchyDeletionZoneEvidence(evidence)
		if encodeErr != nil {
			return nil, encodeErr
		}
		nodes = append(nodes, hierarchyDeletionControllerNode(
			"zone:"+evidence.ZoneID+":remove", "zone", evidence.ZoneID,
			HierarchyDeletionZoneRemove, projection.Revision, prerequisites,
			"zone.remove", hierarchyDeletionBytesDigest(value),
		))
		clear(value)
	}
	routes := make([]HierarchyDeletionMembershipNode, 0, len(projection.Record.DesiredRoutes))
	if projection.Revision != 0 {
		digest, digestErr := EnvironmentBlueprintDependencyDigest(projection.Record)
		if digestErr != nil {
			return nil, digestErr
		}
		inputDigest := hex.EncodeToString(digest[:])
		for _, route := range projection.Record.DesiredRoutes {
			routes = append(routes, hierarchyDeletionControllerNode(
				"route:"+route.Desired.ID+":remove", "route", route.Desired.ID,
				HierarchyDeletionRouteRemove, projection.Revision, nil, "route.remove", inputDigest,
			))
		}
	}
	prerequisites = terminalHierarchyDeletionNodes(nodes)
	for index := range routes {
		routes[index].PrerequisiteNodeIDs = append([]string(nil), prerequisites...)
	}
	nodes = append(nodes, routes...)
	reservation := hierarchyDeletionControllerNode(
		"reservation:"+environmentID+":release", "reservation", environmentID,
		HierarchyDeletionReservationRelease, environmentRevision,
		terminalHierarchyDeletionNodes(nodes), "reservation.release", environmentDigest,
	)
	nodes = append(nodes, reservation)
	return nodes, nil
}
func (repository *HierarchyDeletionRepository) freezeEnvironmentServiceRuntimeMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	projection etcdstore.Versioned[EnvironmentComposeProjection],
) ([]HierarchyDeletionMembershipNode, error) {
	if projection.Revision == 0 {
		return nil, nil
	}
	environmentID := projection.Record.EnvironmentID
	nodes := make([]HierarchyDeletionMembershipNode, 0, len(projection.Record.DesiredServices))
	for begin := 0; begin < len(projection.Record.DesiredServices); begin += 32 {
		end := min(begin+32, len(projection.Record.DesiredServices))
		keys := make([]string, end-begin)
		for index, desired := range projection.Record.DesiredServices[begin:end] {
			keys[index] = serviceRuntimeKey(desired.Desired.ID)
		}
		read, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: keys, Revision: operation.Tombstone.SnapshotRevision,
		})
		if readErr != nil {
			return nil, readErr
		}
		if read == nil || read.ReadRevision != operation.Tombstone.SnapshotRevision ||
			len(read.Values) != len(keys) {
			if read != nil {
				clearKeyValues(read.Values)
			}
			return nil, corruptHierarchyDeletion()
		}
		for index, value := range read.Values {
			if value == nil {
				continue
			}
			desired := projection.Record.DesiredServices[begin+index]
			runtime, decodeErr := decodeServiceRuntimeRecord(value.Value)
			if decodeErr != nil || value.Key != keys[index] || runtime.EnvironmentID != environmentID ||
				runtime.ServiceID != desired.Desired.ID || runtime.BackingNetworkID != desired.BackingNetworkID {
				clearKeyValues(read.Values)
				return nil, corruptHierarchyDeletion()
			}
			nodes = append(nodes, hierarchyDeletionControllerNode(
				fmt.Sprintf("service:%s:%s", runtime.ServiceID, HierarchyDeletionServiceRemove),
				"service",
				runtime.ServiceID,
				HierarchyDeletionServiceRemove,
				value.ModRevision,
				nil,
				"service.remove",
				hierarchyDeletionBytesDigest(value.Value),
			))
		}
		clearKeyValues(read.Values)
	}
	return nodes, nil
}
