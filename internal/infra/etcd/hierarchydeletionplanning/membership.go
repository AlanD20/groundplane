package hierarchydeletionplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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

func (repository *Planner) FreezeCapturedMembership(
	ctx context.Context,
	current hierarchydeletion.HierarchyDeletionTombstone,
) (HierarchyDeletionFrozenMembership, error) {
	frozen := HierarchyDeletionFrozenMembership{
		Revision: current.SnapshotRevision, CoordinationEpoch: current.DeletionEpoch,
		RootRevision:   current.TargetRevision,
		RootTargetKind: hierarchydeletion.HierarchyDeletionActionTargetKind(current.TargetKind),
	}
	rootFinalizer := ""
	switch current.OperationKind {
	case hierarchydeletion.HierarchyDeletionOperationTenant:
		rootFinalizer = "tenant.finalize"
	case hierarchydeletion.HierarchyDeletionOperationProject:
		rootFinalizer = "project.finalize"
	case hierarchydeletion.HierarchyDeletionOperationEnvironment:
		rootFinalizer = "environment.finalize"
	case hierarchydeletion.HierarchyDeletionOperationBacking:
		rootFinalizer = "backing.finalize"
		frozen.RootTargetKind = hierarchydeletion.HierarchyDeletionActionTargetKind("backing-service")
	default:
		return HierarchyDeletionFrozenMembership{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	rootDigest, err := repository.hierarchyDeletionTargetDigest(
		ctx, current.SnapshotRevision, frozen.RootTargetKind,
		current.TargetID, frozen.RootRevision,
	)
	if err != nil {
		return HierarchyDeletionFrozenMembership{}, err
	}
	frozen.RootProcedureInput = hierarchyDeletionControllerInput(
		rootFinalizer, frozen.RootTargetKind, current.TargetID,
		frozen.RootRevision, rootDigest,
	)
	switch current.TargetKind {
	case hierarchydeletion.HierarchyDeletionTargetEnvironment:
		frozen.Nodes, err = repository.freezeEnvironmentMembership(
			ctx, current, current.TargetID, current.TargetRevision, rootDigest,
		)
	case hierarchydeletion.HierarchyDeletionTargetProject:
		frozen.Nodes, err = repository.freezeProjectMembership(
			ctx, current, current.TargetID, true,
		)
	case hierarchydeletion.HierarchyDeletionTargetBacking:
		frozen.Nodes, err = repository.freezeProjectMembership(
			ctx, current, current.TargetID, true,
		)
	case hierarchydeletion.HierarchyDeletionTargetTenant:
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
