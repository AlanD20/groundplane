package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type HierarchyDeletionMembershipNode struct {
	NodeID              string
	TargetKind          hierarchydeletion.HierarchyDeletionActionTargetKind
	TargetID            string
	ActionKind          hierarchydeletion.HierarchyDeletionActionKind
	TargetRevision      int64
	PrerequisiteNodeIDs []string
	ProcedureInput      HierarchyDeletionProcedureInput
	fixedInputDigest    string
}

type HierarchyDeletionAgentInput struct {
	TaskType       taskjournal.TaskType
	TypedProcedure string
	InputDigest    string
	TimeoutSeconds int64
}

type HierarchyDeletionControllerFinalizerInput struct {
	Finalizer          string
	TargetKind         hierarchydeletion.HierarchyDeletionActionTargetKind
	TargetID           string
	FixedInputRevision int64
	FixedInputDigest   string
	BatchOrdinal       int64
	BatchCount         int64
}

type HierarchyDeletionProcedureInput struct {
	Kind                hierarchydeletion.HierarchyDeletionProcedureKind
	AgentChild          *HierarchyDeletionAgentInput
	ControllerFinalizer *HierarchyDeletionControllerFinalizerInput
}

type HierarchyDeletionReverseReference struct {
	SourceKind hierarchydeletion.HierarchyDeletionActionTargetKind
	SourceID   string
	TargetKind hierarchydeletion.HierarchyDeletionActionTargetKind
	TargetID   string
}

type HierarchyDeletionFrozenMembership struct {
	Revision                int64
	CoordinationEpoch       int64
	RootRevision            int64
	RootTargetKind          hierarchydeletion.HierarchyDeletionActionTargetKind
	RootProcedureInput      HierarchyDeletionControllerFinalizerInput
	RootPrerequisiteNodeIDs []string
	Nodes                   []HierarchyDeletionMembershipNode
	ReverseReferences       []HierarchyDeletionReverseReference
}

type hierarchyDeletionIndexedResource struct {
	targetKind    hierarchydeletion.HierarchyDeletionActionTargetKind
	actionKind    hierarchydeletion.HierarchyDeletionActionKind
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
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	if current.Tombstone.Phase != hierarchydeletion.HierarchyDeletionPlanning || current.Tombstone.SnapshotRevision <= 0 ||
		current.Tombstone.TargetRevision <= 0 || current.Tombstone.PlanCount != nil || current.Tombstone.PlanDigest != nil {
		return HierarchyDeletionFrozenMembership{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion membership is not freezable",
		)
	}
	frozen := HierarchyDeletionFrozenMembership{
		Revision: current.Tombstone.SnapshotRevision, CoordinationEpoch: current.Tombstone.DeletionEpoch,
		RootRevision:   current.Tombstone.TargetRevision,
		RootTargetKind: hierarchydeletion.HierarchyDeletionActionTargetKind(current.Tombstone.TargetKind),
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
		frozen.RootTargetKind = hierarchydeletion.HierarchyDeletionActionTargetKind("backing-service")
	default:
		return HierarchyDeletionFrozenMembership{}, hierarchydeletion.CorruptHierarchyDeletion()
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
		err = hierarchydeletion.CorruptHierarchyDeletion()
	}
	if err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	frozen.RootPrerequisiteNodeIDs = terminalHierarchyDeletionNodes(frozen.Nodes)
	return frozen, nil
}
