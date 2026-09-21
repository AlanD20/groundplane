package etcd

import (
	"context"
	"fmt"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func hierarchyDeletionAgentNode(
	nodeID string,
	targetKind hierarchydeletion.HierarchyDeletionActionTargetKind,
	targetID string,
	action hierarchydeletion.HierarchyDeletionActionKind,
	targetRevision int64,
	prerequisites []string,
	parentOperationID string,
) HierarchyDeletionMembershipNode {
	taskType := taskjournal.TaskRemove
	if action == hierarchydeletion.HierarchyDeletionAttachDetach {
		taskType = taskjournal.TaskDetach
	}
	return HierarchyDeletionMembershipNode{
		NodeID: nodeID, TargetKind: targetKind, TargetID: targetID, ActionKind: action,
		TargetRevision: targetRevision, PrerequisiteNodeIDs: append([]string(nil), prerequisites...),
		ProcedureInput: HierarchyDeletionProcedureInput{
			Kind: hierarchydeletion.HierarchyDeletionProcedureAgent,
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
	targetKind hierarchydeletion.HierarchyDeletionActionTargetKind,
	targetID string,
	action hierarchydeletion.HierarchyDeletionActionKind,
	targetRevision int64,
	prerequisites []string,
	finalizer string,
	fixedInputDigest string,
) HierarchyDeletionMembershipNode {
	return HierarchyDeletionMembershipNode{
		NodeID: nodeID, TargetKind: targetKind, TargetID: targetID, ActionKind: action,
		TargetRevision: targetRevision, PrerequisiteNodeIDs: append([]string(nil), prerequisites...),
		ProcedureInput: HierarchyDeletionProcedureInput{
			Kind: hierarchydeletion.HierarchyDeletionProcedureController,
			ControllerFinalizer: ptrHierarchyDeletionControllerInput(
				hierarchyDeletionControllerInput(finalizer, targetKind, targetID, targetRevision, fixedInputDigest),
			),
		},
	}
}

func hierarchyDeletionControllerInput(
	finalizer string,
	targetKind hierarchydeletion.HierarchyDeletionActionTargetKind,
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

func ptrHierarchyDeletionControllerInput(
	value HierarchyDeletionControllerFinalizerInput,
) *HierarchyDeletionControllerFinalizerInput {
	return &value
}

func (repository *HierarchyDeletionRepository) hierarchyDeletionTargetDigest(
	ctx context.Context,
	revision int64,
	targetKind hierarchydeletion.HierarchyDeletionActionTargetKind,
	targetID string,
	targetRevision int64,
) (string, error) {
	key := ""
	switch targetKind {
	case "tenant":
		key = hierarchyrecord.TenantKey(targetID)
	case "project", "backing-service":
		key = hierarchyrecord.ProjectKey(targetID)
	case "environment", "reservation":
		key = hierarchyrecord.EnvironmentKey(targetID)
	case "script":
		storage, scriptErr := scriptrecord.ReadActiveScriptStorage(ctx, repository.store, targetID, revision)
		if scriptErr != nil || storage.Script.Revision != targetRevision {
			return "", hierarchydeletion.CorruptHierarchyDeletion()
		}
		encoded, encodeErr := scriptrecord.EncodeRecord(storage.Script.Record)
		if encodeErr != nil {
			return "", encodeErr
		}
		digest := hierarchyDeletionBytesDigest(encoded)
		clear(encoded)
		return digest, nil
	case "connector":
		key = connectorrecord.RecordKey(targetID)
	case "runner":
		key = runners.RunnerKey(targetID)
	case "secret":
		key = secretrecord.RecordKey(targetID)
	default:
		return "", errs.Newf(
			errs.KindValidationFailed,
			"hierarchy deletion finalizer target kind %q is unsupported",
			targetKind,
		)
	}
	stored, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return "", err
	}
	if stored == nil || stored.ReadRevision != revision || len(stored.Values) != 1 ||
		stored.Values[0] == nil || stored.Values[0].Key != key || stored.Values[0].ModRevision != targetRevision {
		return "", hierarchydeletion.CorruptHierarchyDeletion()
	}
	digest := hierarchyDeletionBytesDigest(stored.Values[0].Value)
	etcdstore.ClearValues(stored.Values)
	return digest, nil
}
