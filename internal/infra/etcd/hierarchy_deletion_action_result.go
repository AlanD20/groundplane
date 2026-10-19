package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
)

func hierarchyActionCompletion(
	task workexecution.Task,
	item workexecution.Item,
	captured HierarchyWorkInput,
	at time.Time,
) ([]byte, workexecution.Input, error) {
	action := captured.Action
	procedure := action.ControllerProcedure
	proof := hierarchydeletion.HierarchyDeletionActionCompletion{
		Schema: 1, ParentOperationID: action.ParentOperationID,
		DeletionEpoch: captured.Operation.Tombstone.DeletionEpoch, Ordinal: action.Ordinal,
		ActionDigest: item.Input.SHA256, TargetKind: action.TargetKind,
		TargetID: action.TargetID, TargetRevision: action.TargetRevision,
		Executor: hierarchydeletion.HierarchyDeletionProcedureController,
		ControllerProof: &hierarchydeletion.HierarchyDeletionControllerCompletionProof{
			Finalizer: procedure.Finalizer, FixedInputRevision: procedure.FixedInputRevision,
			CompareTemplateDigest:       procedure.CompareTemplateDigest,
			MutationTemplateDigest:      procedure.MutationTemplateDigest,
			PostconditionTemplateDigest: procedure.PostconditionTemplateDigest,
		}, CompletedAt: at,
	}
	value, err := hierarchydeletion.EncodeHierarchyDeletionRecord(
		proof,
		hierarchydeletion.HierarchyDeletionCompletionRecordBytes,
	)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	result := workexecution.Input{
		ID:     ids.DeriveAt(ids.KindPlan, task.CreatedAt, item.ID, "hierarchy-result"),
		SHA256: hierarchydeletion.HierarchyDeletionBytesDigest(value),
	}
	return value, result, nil
}
