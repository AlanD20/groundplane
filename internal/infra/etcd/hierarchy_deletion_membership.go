package etcd

import (
	"context"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type HierarchyDeletionMembershipNode struct {
	NodeID              string
	TargetKind          HierarchyDeletionActionTargetKind
	TargetID            string
	ActionKind          HierarchyDeletionActionKind
	TargetRevision      int64
	PrerequisiteNodeIDs []string
	ProcedureInput      HierarchyDeletionProcedureInput
	fixedInputDigest    string
}

type HierarchyDeletionAgentInput struct {
	TaskType       TaskType
	TypedProcedure string
	InputDigest    string
	TimeoutSeconds int64
}

type HierarchyDeletionControllerFinalizerInput struct {
	Finalizer          string
	TargetKind         HierarchyDeletionActionTargetKind
	TargetID           string
	FixedInputRevision int64
	FixedInputDigest   string
	BatchOrdinal       int64
	BatchCount         int64
}

type HierarchyDeletionProcedureInput struct {
	Kind                HierarchyDeletionProcedureKind
	AgentChild          *HierarchyDeletionAgentInput
	ControllerFinalizer *HierarchyDeletionControllerFinalizerInput
}

type HierarchyDeletionReverseReference struct {
	SourceKind HierarchyDeletionActionTargetKind
	SourceID   string
	TargetKind HierarchyDeletionActionTargetKind
	TargetID   string
}

type HierarchyDeletionFrozenMembership struct {
	Revision                int64
	CoordinationEpoch       int64
	RootRevision            int64
	RootTargetKind          HierarchyDeletionActionTargetKind
	RootProcedureInput      HierarchyDeletionControllerFinalizerInput
	RootPrerequisiteNodeIDs []string
	Nodes                   []HierarchyDeletionMembershipNode
	ReverseReferences       []HierarchyDeletionReverseReference
}

type hierarchyDeletionIndexedResource struct {
	targetKind    HierarchyDeletionActionTargetKind
	actionKind    HierarchyDeletionActionKind
	ownerPrefix   func(string) string
	primaryKey    func(string) string
	stableIDKind  ids.Kind
	validateOwner func([]byte, string, string) error
	controller    bool
}

func (repository *HierarchyDeletionRepository) FreezeMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) (HierarchyDeletionFrozenMembership, error) {
	if err := validateContext(ctx); err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	if current.Tombstone.Phase != HierarchyDeletionPlanning || current.Tombstone.SnapshotRevision <= 0 ||
		current.Tombstone.TargetRevision <= 0 || current.Tombstone.PlanCount != nil || current.Tombstone.PlanDigest != nil {
		return HierarchyDeletionFrozenMembership{}, errs.New(errs.KindStateConflict, "hierarchy deletion membership is not freezable")
	}
	frozen := HierarchyDeletionFrozenMembership{
		Revision: current.Tombstone.SnapshotRevision, CoordinationEpoch: current.Tombstone.DeletionEpoch,
		RootRevision:   current.Tombstone.TargetRevision,
		RootTargetKind: HierarchyDeletionActionTargetKind(current.Tombstone.TargetKind),
	}
	rootFinalizer := ""
	switch current.Tombstone.OperationKind {
	case HierarchyDeletionOperationTenant:
		rootFinalizer = "tenant.finalize"
	case HierarchyDeletionOperationProject:
		rootFinalizer = "project.finalize"
	case HierarchyDeletionOperationEnvironment:
		rootFinalizer = "environment.finalize"
	case HierarchyDeletionOperationBacking:
		rootFinalizer = "backing.finalize"
		frozen.RootTargetKind = HierarchyDeletionActionTargetKind("backing-service")
	default:
		return HierarchyDeletionFrozenMembership{}, corruptHierarchyDeletion()
	}
	rootDigest, err := repository.hierarchyDeletionTargetDigest(
		ctx, current.Tombstone.SnapshotRevision, frozen.RootTargetKind,
		current.Tombstone.TargetID, frozen.RootRevision,
	)
	if err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	frozen.RootProcedureInput = hierarchyDeletionControllerInput(
		rootFinalizer, frozen.RootTargetKind, current.Tombstone.TargetID,
		frozen.RootRevision, rootDigest,
	)
	switch current.Tombstone.TargetKind {
	case HierarchyDeletionTargetEnvironment:
		frozen.Nodes, err = repository.freezeEnvironmentMembership(
			ctx, current, current.Tombstone.TargetID, current.Tombstone.TargetRevision, rootDigest,
		)
	case HierarchyDeletionTargetProject:
		frozen.Nodes, err = repository.freezeProjectMembership(
			ctx, current, current.Tombstone.TargetID, true,
		)
	case HierarchyDeletionTargetBacking:
		frozen.Nodes, err = repository.freezeProjectMembership(
			ctx, current, current.Tombstone.TargetID, true,
		)
	case HierarchyDeletionTargetTenant:
		frozen.Nodes, err = repository.freezeTenantMembership(ctx, current)
	default:
		err = corruptHierarchyDeletion()
	}
	if err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	frozen.RootPrerequisiteNodeIDs = terminalHierarchyDeletionNodes(frozen.Nodes)
	return frozen, nil
}

func (repository *HierarchyDeletionRepository) freezeTenantMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
) ([]HierarchyDeletionMembershipNode, error) {
	projects, err := repository.hierarchyDeletionIndexedTargets(
		ctx, operation.Tombstone.SnapshotRevision, projectTenantOwnerPrefix(operation.Tombstone.TargetID),
		projectKey, ids.KindProject, func(value []byte, id, owner string) error {
			record, decodeErr := decodeProject(value)
			if decodeErr != nil || record.ID != id || record.TenantID != owner || record.Kind != ProjectKindTenant {
				return corruptHierarchyDeletion()
			}
			return nil
		}, operation.Tombstone.TargetID,
	)
	if err != nil {
		return nil, err
	}
	nodes := make([]HierarchyDeletionMembershipNode, 0)
	for _, project := range projects {
		children, freezeErr := repository.freezeProjectMembership(ctx, operation, project.id, false)
		if freezeErr != nil {
			return nil, freezeErr
		}
		nodes = append(nodes, children...)
		nodes = append(nodes, hierarchyDeletionControllerNode(
			"project:"+project.id+":finalize", "project", project.id,
			HierarchyDeletionProjectFinalize, project.revision,
			terminalHierarchyDeletionNodes(children), "project.finalize", project.digest,
		))
	}
	runners, err := repository.freezeIndexedResource(ctx, operation, operation.Tombstone.TargetID,
		hierarchyDeletionIndexedResource{
			targetKind: "runner", actionKind: HierarchyDeletionRunnerLocalRemove,
			ownerPrefix: func(owner string) string { return runnerOwnerPrefix(RunnerOwnerTenant, owner) },
			primaryKey:  runnerKey, stableIDKind: ids.KindRunner, controller: true,
			validateOwner: validateHierarchyDeletionRunnerOwner,
		})
	if err != nil {
		return nil, err
	}
	return append(nodes, runners...), nil
}

func (repository *HierarchyDeletionRepository) freezeProjectMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	projectID string,
	root bool,
) ([]HierarchyDeletionMembershipNode, error) {
	environments, err := repository.hierarchyDeletionIndexedTargets(
		ctx, operation.Tombstone.SnapshotRevision, environmentOwnerPrefix(projectID), environmentKey,
		ids.KindEnvironment, func(value []byte, id, owner string) error {
			record, decodeErr := decodeEnvironment(value)
			if decodeErr != nil || record.ID != id || record.ProjectID != owner {
				return corruptHierarchyDeletion()
			}
			return nil
		}, projectID,
	)
	if err != nil {
		return nil, err
	}
	nodes := make([]HierarchyDeletionMembershipNode, 0)
	for _, environment := range environments {
		children, freezeErr := repository.freezeEnvironmentMembership(ctx, operation, environment.id, environment.revision, environment.digest)
		if freezeErr != nil {
			return nil, freezeErr
		}
		nodes = append(nodes, children...)
		nodes = append(nodes, hierarchyDeletionControllerNode(
			"environment:"+environment.id+":finalize", "environment", environment.id,
			HierarchyDeletionEnvironmentFinalize, environment.revision,
			terminalHierarchyDeletionNodes(children), "environment.finalize", environment.digest,
		))
	}
	if operation.Tombstone.OperationKind == HierarchyDeletionOperationBacking && root {
		return nodes, nil
	}
	runners, err := repository.freezeIndexedResource(ctx, operation, projectID,
		hierarchyDeletionIndexedResource{
			targetKind: "runner", actionKind: HierarchyDeletionRunnerLocalRemove,
			ownerPrefix: func(owner string) string { return runnerOwnerPrefix(RunnerOwnerProject, owner) },
			primaryKey:  runnerKey, stableIDKind: ids.KindRunner, controller: true,
			validateOwner: validateHierarchyDeletionRunnerOwner,
		})
	if err != nil {
		return nil, err
	}
	nodes = append(nodes, runners...)
	secrets, err := repository.freezeIndexedResource(ctx, operation, projectID,
		hierarchyDeletionIndexedResource{
			targetKind: "secret", actionKind: HierarchyDeletionProjectSecretRemove,
			ownerPrefix: func(owner string) string {
				return secretOwnerCollectionPrefix(core.SecretScopeProject, owner)
			},
			primaryKey: secretRecordKey, stableIDKind: ids.KindSecret, controller: true,
			validateOwner: validateHierarchyDeletionSecretOwner,
		})
	if err != nil {
		return nil, err
	}
	return append(nodes, secrets...), nil
}

func (repository *HierarchyDeletionRepository) freezeEnvironmentMembership(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	environmentID string,
	environmentRevision int64,
	environmentDigest string,
) ([]HierarchyDeletionMembershipNode, error) {
	descriptors := []hierarchyDeletionIndexedResource{
		{targetKind: "attach", actionKind: HierarchyDeletionAttachGrantRevoke, ownerPrefix: attachOwnerPrefix, primaryKey: attachKey, stableIDKind: ids.KindAttach, validateOwner: validateHierarchyDeletionAttachOwner},
		{targetKind: "service", actionKind: HierarchyDeletionServiceRemove, ownerPrefix: serviceOwnerPrefix, primaryKey: serviceKey, stableIDKind: ids.KindService, validateOwner: validateHierarchyDeletionServiceOwner},
		{targetKind: "entry", actionKind: HierarchyDeletionEntryRemove, ownerPrefix: entryOwnerCollectionPrefix, primaryKey: entryRecordKey, stableIDKind: ids.KindEnvEntry, validateOwner: validateHierarchyDeletionEntryOwner},
		{targetKind: "route", actionKind: HierarchyDeletionRouteRemove, ownerPrefix: routeOwnerPrefix, primaryKey: routeKey, stableIDKind: ids.KindRoute, validateOwner: validateHierarchyDeletionRouteOwner},
		{targetKind: "component", actionKind: HierarchyDeletionComponentRemove, ownerPrefix: componentEnvironmentOwnerPrefix, primaryKey: componentKey, stableIDKind: ids.KindComponent, validateOwner: validateHierarchyDeletionComponentOwner},
		{targetKind: "script", actionKind: HierarchyDeletionScriptRemove, ownerPrefix: scriptOwnerPrefix, primaryKey: scriptKey, stableIDKind: ids.KindScript, validateOwner: validateHierarchyDeletionScriptOwner, controller: true},
		{targetKind: "volume", actionKind: HierarchyDeletionVolumeAgentCleanup, ownerPrefix: volumeOwnerPrefix, primaryKey: volumeKey, stableIDKind: ids.KindVolume, validateOwner: validateHierarchyDeletionVolumeOwner},
		{targetKind: "zone", actionKind: HierarchyDeletionZoneRemove, ownerPrefix: zoneOwnerPrefix, primaryKey: zoneKey, stableIDKind: ids.KindNetwork, validateOwner: validateHierarchyDeletionZoneOwner},
		{targetKind: "connector", actionKind: HierarchyDeletionConnectorFinalize, ownerPrefix: connectorEnvironmentPrefix, primaryKey: connectorRecordKey, stableIDKind: ids.KindConnector, validateOwner: validateHierarchyDeletionConnectorOwner, controller: true},
	}
	nodes := make([]HierarchyDeletionMembershipNode, 0)
	for _, descriptor := range descriptors {
		part, err := repository.freezeIndexedResource(ctx, operation, environmentID, descriptor)
		if err != nil {
			return nil, err
		}
		if descriptor.actionKind == HierarchyDeletionAttachGrantRevoke {
			for _, grant := range part {
				nodes = append(nodes, grant)
				detach := hierarchyDeletionAgentNode(
					"attach:"+grant.TargetID+":detach", "attach", grant.TargetID,
					HierarchyDeletionAttachDetach, grant.TargetRevision, []string{grant.NodeID}, operation.Tombstone.OperationID,
				)
				detach.fixedInputDigest = grant.fixedInputDigest
				nodes = append(nodes, detach)
			}
			continue
		}
		if descriptor.actionKind == HierarchyDeletionVolumeAgentCleanup {
			for _, cleanup := range part {
				nodes = append(nodes, cleanup)
				nodes = append(nodes, hierarchyDeletionControllerNode(
					"volume:"+cleanup.TargetID+":finalize", "volume", cleanup.TargetID,
					HierarchyDeletionVolumeFinalize, cleanup.TargetRevision, []string{cleanup.NodeID}, "volume.finalize",
					cleanup.fixedInputDigest,
				))
			}
			continue
		}
		nodes = append(nodes, part...)
	}
	reservation := hierarchyDeletionControllerNode(
		"reservation:"+environmentID+":release", "reservation", environmentID,
		HierarchyDeletionReservationRelease, environmentRevision,
		terminalHierarchyDeletionNodes(nodes), "reservation.release", environmentDigest,
	)
	nodes = append(nodes, reservation)
	cleanup := hierarchyDeletionAgentNode(
		"environment:"+environmentID+":cleanup", "environment", environmentID,
		HierarchyDeletionEnvironmentAgentCleanup, environmentRevision,
		terminalHierarchyDeletionNodes(nodes), operation.Tombstone.OperationID,
	)
	cleanup.fixedInputDigest = environmentDigest
	nodes = append(nodes, cleanup)
	return nodes, nil
}

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
		page, err := repository.store.Range(ctx, RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: 128, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision {
			return nil, corruptHierarchyDeletion()
		}
		for _, value := range page.Values {
			if validateListKey(prefix, value.Key, kind) != nil {
				clearRangeValues(page.Values)
				return nil, corruptHierarchyDeletion()
			}
			id := strings.TrimPrefix(value.Key, prefix)
			if string(value.Value) != id {
				clearRangeValues(page.Values)
				return nil, corruptHierarchyDeletion()
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
			return nil, corruptHierarchyDeletion()
		}
	}
	targets := make([]hierarchyDeletionIndexedTarget, 0, len(idsAtRevision))
	for begin := 0; begin < len(idsAtRevision); begin += 32 {
		end := min(begin+32, len(idsAtRevision))
		keys := make([]string, end-begin)
		for index, id := range idsAtRevision[begin:end] {
			keys[index] = primaryKey(id)
		}
		read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
			return nil, corruptHierarchyDeletion()
		}
		for index, value := range read.Values {
			id := idsAtRevision[begin+index]
			if value == nil || value.Key != keys[index] || value.ModRevision <= 0 ||
				validateOwner(value.Value, id, ownerID) != nil {
				clearKeyValues(read.Values)
				return nil, corruptHierarchyDeletion()
			}
			targets = append(targets, hierarchyDeletionIndexedTarget{
				id: id, revision: value.ModRevision, digest: hierarchyDeletionBytesDigest(value.Value),
			})
		}
		clearKeyValues(read.Values)
	}
	return targets, nil
}

func hierarchyDeletionAgentNode(
	nodeID string,
	targetKind HierarchyDeletionActionTargetKind,
	targetID string,
	action HierarchyDeletionActionKind,
	targetRevision int64,
	prerequisites []string,
	parentOperationID string,
) HierarchyDeletionMembershipNode {
	taskType := TaskRemove
	if action == HierarchyDeletionAttachDetach {
		taskType = TaskDetach
	}
	return HierarchyDeletionMembershipNode{
		NodeID: nodeID, TargetKind: targetKind, TargetID: targetID, ActionKind: action,
		TargetRevision: targetRevision, PrerequisiteNodeIDs: append([]string(nil), prerequisites...),
		ProcedureInput: HierarchyDeletionProcedureInput{
			Kind: HierarchyDeletionProcedureAgent,
			AgentChild: &HierarchyDeletionAgentInput{
				TaskType:       taskType,
				TypedProcedure: hierarchyDeletionAgentProcedure(action),
				InputDigest: hierarchyDeletionFoldDigest(
					"groundplane-deletion-agent-input-v1", parentOperationID, nodeID,
					string(action), string(targetKind), targetID, fmt.Sprint(targetRevision),
				),
				TimeoutSeconds: int64(hierarchyDeletionAttemptTimeout.Seconds()),
			},
		},
	}
}

func hierarchyDeletionControllerNode(
	nodeID string,
	targetKind HierarchyDeletionActionTargetKind,
	targetID string,
	action HierarchyDeletionActionKind,
	targetRevision int64,
	prerequisites []string,
	finalizer string,
	fixedInputDigest string,
) HierarchyDeletionMembershipNode {
	return HierarchyDeletionMembershipNode{
		NodeID: nodeID, TargetKind: targetKind, TargetID: targetID, ActionKind: action,
		TargetRevision: targetRevision, PrerequisiteNodeIDs: append([]string(nil), prerequisites...),
		ProcedureInput: HierarchyDeletionProcedureInput{
			Kind: HierarchyDeletionProcedureController,
			ControllerFinalizer: ptrHierarchyDeletionControllerInput(
				hierarchyDeletionControllerInput(finalizer, targetKind, targetID, targetRevision, fixedInputDigest),
			),
		},
	}
}

func hierarchyDeletionControllerInput(
	finalizer string,
	targetKind HierarchyDeletionActionTargetKind,
	targetID string,
	targetRevision int64,
	fixedInputDigest string,
) HierarchyDeletionControllerFinalizerInput {
	return HierarchyDeletionControllerFinalizerInput{
		Finalizer: finalizer, FixedInputRevision: targetRevision,
		TargetKind: targetKind, TargetID: targetID, FixedInputDigest: fixedInputDigest,
		BatchOrdinal: 0, BatchCount: 1,
	}
}

func ptrHierarchyDeletionControllerInput(value HierarchyDeletionControllerFinalizerInput) *HierarchyDeletionControllerFinalizerInput {
	return &value
}

func (repository *HierarchyDeletionRepository) hierarchyDeletionTargetDigest(
	ctx context.Context,
	revision int64,
	targetKind HierarchyDeletionActionTargetKind,
	targetID string,
	targetRevision int64,
) (string, error) {
	key := ""
	switch targetKind {
	case "tenant":
		key = tenantKey(targetID)
	case "project", "backing-service":
		key = projectKey(targetID)
	case "environment", "reservation":
		key = environmentKey(targetID)
	case "script":
		key = scriptKey(targetID)
	case "volume":
		key = volumeKey(targetID)
	case "connector":
		key = connectorRecordKey(targetID)
	case "runner":
		key = runnerKey(targetID)
	case "secret":
		key = secretRecordKey(targetID)
	default:
		return "", errs.Newf(errs.KindValidationFailed, "hierarchy deletion finalizer target kind %q is unsupported", targetKind)
	}
	stored, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return "", err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != 1 ||
		stored.Values[0] == nil || stored.Values[0].Key != key || stored.Values[0].ModRevision != targetRevision {
		return "", corruptHierarchyDeletion()
	}
	digest := hierarchyDeletionBytesDigest(stored.Values[0].Value)
	clearKeyValues(stored.Values)
	return digest, nil
}
