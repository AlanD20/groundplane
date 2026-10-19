package serviceremoval

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *WorkReports) CompleteLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceLifecycleCompletionRequest,
) (*agentpb.ServiceLifecycleCompletionAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Service removal completion evidence is required")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	evidence := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	view, input, err := service.forwardReport(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	if view.Checkpoint.Record == nil || view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(
			errs.KindStateConflict,
			"Service removal completion differs from its acknowledged checkpoint",
		)
	}
	if view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked {
		// Require complete host proof before interpreting a completion report as
		// a stopped action. The failure owner atomically captures restoration and
		// preserves the deletion fence rather than publishing obsolete intent.
		if view.Attempt.Attempt.FinishedAt == nil {
			_, proof, err := service.inputs.PrepareHostProof(ctx, view, input, time.Now().UTC())
			keyvalue.ClearMutationValues(proof.Mutations)
			if err != nil {
				return nil, err
			}
		}
		stopped, err := service.FailLifecycle(ctx, authorization, &agentpb.ServiceLifecycleFailureRequest{
			Attempt: request.Attempt, Reason: string(lifecycleexecution.ExecutionInterrupted)})
		if err != nil {
			return nil, err
		}
		return &agentpb.ServiceLifecycleCompletionAck{Stopped: stopped}, nil
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, err := service.records.PrepareServiceRemovalCompletion(ctx, view, input, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(proof.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result = workexecution.Completion{
			Outcome:        workexecution.Succeeded,
			Evidence:       evidence,
			EffectsSettled: true,
		}
		completion.At = at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return nil, err
		}
		if !result.Succeeded {
			return nil, errs.New(
				errs.KindStateConflict,
				"Service removal completion ownership changed; resend the saved result",
			)
		}
		view, _, err = service.forwardReport(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || *attempt.Result != evidence ||
		attempt.Outcome != workexecution.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Saved Service removal result does not acknowledge completion")
	}
	return &agentpb.ServiceLifecycleCompletionAck{
		Attempt:  proto.CloneOf(request.Attempt),
		Evidence: proto.CloneOf(request.Evidence),
		StartedAt: timestamppb.New(
			*attempt.StartedAt,
		),
		FinishedAt: timestamppb.New(*attempt.FinishedAt),
		Outcome:    string(attempt.Outcome),
	}, nil
}
