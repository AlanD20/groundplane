package backup

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (owner *AgentWork) Recover(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackupRecoveryRequest,
) (*agentpb.BackupRecoveryAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || len(request.Report) == 0 || len(request.Report) > executionplan.MaximumBackupRecoveryReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "Backup prune recovery requires its bounded executor-stop report")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	attempt, err := workexecution.DecodeAgentAttempt(request.Attempt, authorization.Identity())
	if err != nil {
		return nil, err
	}
	view, err := owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, authorization.Identity())
	if err != nil {
		return nil, err
	}
	if !workexecution.SameAssignment(view.Attempt.Attempt, attempt) || attempt.Direction != workexecution.Recovery ||
		(view.Work.Item.Item.Operation != workexecution.BackupPrune && view.Work.Item.Item.Operation != workexecution.Backup && view.Work.Item.Item.Operation != workexecution.Restore) || view.Attempt.Attempt.StartedAt == nil {
		return nil, errs.New(errs.KindStateConflict, "Backup prune report differs from its started recovery")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, result, err := owner.inputs.PrepareRecoverySettlement(ctx, view, request.Report, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		completion, err := owner.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result, completion.At = result, at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		committed, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "Backup recovery ownership changed; resend the saved report")
		}
		view, err = owner.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, authorization.Identity())
		if err != nil {
			return nil, err
		}
	}
	raw, err := owner.inputs.LoadRecoveryReport(ctx, view)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(raw, request.Report) {
		return nil, errs.New(errs.KindStateConflict, "Backup recovery replay differs from its saved report")
	}
	saved := view.Attempt.Attempt
	digest, err := hex.DecodeString(saved.Result.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackupRecoveryAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence:  &agentpb.WorkInputReference{InputId: saved.Result.ID, Sha256: digest},
		StartedAt: timestamppb.New(*saved.StartedAt), FinishedAt: timestamppb.New(*saved.FinishedAt)}, nil
}
