package etcd

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
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
		return nil, hierarchydeletion.CorruptHierarchyDeletion()
	}
	descriptors := []hierarchyDeletionIndexedResource{
		{
			targetKind:    "attach",
			actionKind:    hierarchydeletion.HierarchyDeletionAttachGrantRevoke,
			ownerPrefix:   attachrecord.AttachOwnerPrefix,
			primaryKey:    attachrecord.AttachKey,
			stableIDKind:  ids.KindAttach,
			validateOwner: validateHierarchyDeletionAttachOwner,
		},
		{
			targetKind:    "release-group",
			actionKind:    hierarchydeletion.HierarchyDeletionReleaseGroupRemove,
			ownerPrefix:   func(owner string) string { return releaseGroupOwnerPrefix + owner + "/" },
			primaryKey:    releaseGroupRecordKey,
			stableIDKind:  ids.KindReleaseGroup,
			validateOwner: validateHierarchyDeletionReleaseGroupOwner,
			controller:    true,
		},
		{
			targetKind:    "entry",
			actionKind:    hierarchydeletion.HierarchyDeletionEntryRemove,
			ownerPrefix:   entryOwnerCollectionPrefix,
			primaryKey:    entryrecord.RecordKey,
			stableIDKind:  ids.KindEnvEntry,
			validateOwner: validateHierarchyDeletionEntryOwner,
			controller:    true,
		},
		{
			targetKind:    "component",
			actionKind:    hierarchydeletion.HierarchyDeletionComponentRemove,
			ownerPrefix:   componentrecord.EnvironmentOwnerPrefix,
			primaryKey:    componentrecord.RecordKey,
			stableIDKind:  ids.KindComponent,
			validateOwner: validateHierarchyDeletionComponentOwner,
			controller:    true,
		},
		{targetKind: "script", actionKind: hierarchydeletion.HierarchyDeletionScriptRemove,
			ownerPrefix: func(owner string) string {
				return scriptrecord.ScriptSetOwnerPrefix(owner, activeScripts.Record.GenerationID)
			},
			primaryKey: func(id string) string {
				return scriptrecord.ScriptSetScriptKey(environmentID, activeScripts.Record.GenerationID, id)
			},
			stableIDKind: ids.KindScript, validateOwner: validateHierarchyDeletionScriptOwner, controller: true},
		{
			targetKind:    "connector",
			actionKind:    hierarchydeletion.HierarchyDeletionConnectorFinalize,
			ownerPrefix:   connectorEnvironmentPrefix,
			primaryKey:    connectorrecord.RecordKey,
			stableIDKind:  ids.KindConnector,
			validateOwner: validateHierarchyDeletionConnectorOwner,
			controller:    true,
		},
	}
	cleanup := hierarchyDeletionAgentNode(
		"environment:"+environmentID+":cleanup", "environment", environmentID,
		hierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup, environmentRevision, nil, operation.Tombstone.OperationID,
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
		if descriptor.actionKind == hierarchydeletion.HierarchyDeletionAttachGrantRevoke {
			for _, grant := range part {
				nodes = append(nodes, grant)
				detach := hierarchyDeletionAgentNode(
					"attach:"+grant.TargetID+":detach",
					"attach",
					grant.TargetID,
					hierarchydeletion.HierarchyDeletionAttachDetach,
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
		if descriptor.actionKind == hierarchydeletion.HierarchyDeletionReleaseGroupRemove {
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
			hierarchydeletion.HierarchyDeletionZoneRemove, projection.Revision, prerequisites,
			"zone.remove", hierarchyDeletionBytesDigest(value),
		))
		clear(value)
	}
	routes := make([]HierarchyDeletionMembershipNode, 0, len(projection.Record.DesiredRoutes))
	if projection.Revision != 0 {
		digest, digestErr := blueprints.EnvironmentBlueprintDependencyDigest(projection.Record)
		if digestErr != nil {
			return nil, digestErr
		}
		inputDigest := hex.EncodeToString(digest[:])
		for _, route := range projection.Record.DesiredRoutes {
			routes = append(routes, hierarchyDeletionControllerNode(
				"route:"+route.Desired.ID+":remove", "route", route.Desired.ID,
				hierarchydeletion.HierarchyDeletionRouteRemove, projection.Revision, nil, "route.remove", inputDigest,
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
		hierarchydeletion.HierarchyDeletionReservationRelease, environmentRevision,
		terminalHierarchyDeletionNodes(nodes), "reservation.release", environmentDigest,
	)
	nodes = append(nodes, reservation)
	return nodes, nil
}
func (repository *HierarchyDeletionRepository) freezeEnvironmentServiceRuntimeMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
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
			keys[index] = servicerecord.ServiceRuntimeKey(desired.Desired.ID)
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
			return nil, hierarchydeletion.CorruptHierarchyDeletion()
		}
		for index, value := range read.Values {
			if value == nil {
				continue
			}
			desired := projection.Record.DesiredServices[begin+index]
			runtime, decodeErr := servicerecord.DecodeServiceRuntimeRecord(value.Value)
			if decodeErr != nil || value.Key != keys[index] || runtime.EnvironmentID != environmentID ||
				runtime.ServiceID != desired.Desired.ID || runtime.BackingNetworkID != desired.BackingNetworkID {
				clearKeyValues(read.Values)
				return nil, hierarchydeletion.CorruptHierarchyDeletion()
			}
			nodes = append(nodes, hierarchyDeletionControllerNode(
				fmt.Sprintf("service:%s:%s", runtime.ServiceID, hierarchydeletion.HierarchyDeletionServiceRemove),
				"service",
				runtime.ServiceID,
				hierarchydeletion.HierarchyDeletionServiceRemove,
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
