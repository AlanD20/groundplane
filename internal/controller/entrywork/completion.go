package entrywork

import (
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

func (service *WorkReports) CompleteEntry(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.EntryConfigurationWorkCompletionRequest,
) (*agentpb.EntryConfigurationWorkCompletionAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "Entry completion evidence is required")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	evidence := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	if view.Checkpoint.Record == nil || view.Checkpoint.Record.Evidence != evidence {
		return nil, errs.New(errs.KindStateConflict, "Entry completion differs from its acknowledged checkpoint")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		input, _, _, err := service.records.Load(ctx, view.Work)
		if err != nil {
			return nil, err
		}
		at := time.Now().UTC()
		proof, err := service.records.PrepareAppliedState(ctx, view, input, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(proof.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result = workexecution.Completion{
			Outcome:        completedEntryOutcome(view.Work),
			Evidence:       evidence,
			EffectsSettled: true,
		}
		completion.At = at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(change.Mutations)
		result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(result.FailureReads)
		if err != nil {
			return nil, err
		}
		if !result.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "Entry completion ownership changed; resend the saved result")
		}
		view, err = service.reportView(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.FinishedAt == nil || attempt.Result == nil || *attempt.Result != evidence ||
		(attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled && attempt.Outcome != workexecution.Superseded) {
		return nil, errs.New(errs.KindStateConflict, "saved Entry result does not acknowledge successful completion")
	}
	return &agentpb.EntryConfigurationWorkCompletionAck{
		Attempt:  proto.CloneOf(request.Attempt),
		Evidence: proto.CloneOf(request.Evidence),
		StartedAt: timestamppb.New(
			*attempt.StartedAt,
		),
		FinishedAt: timestamppb.New(*attempt.FinishedAt),
		Outcome:    string(attempt.Outcome),
	}, nil
}

// A proven host effect remains applied, but cannot turn cancelled or superseded
// intent back into a successful action. The acknowledgement confirms receipt of
// that effect, not permission to continue the old operation.
func completedEntryOutcome(view workqueue.View) workexecution.Outcome {
	if view.Task.Task.Stop != nil {
		return view.Task.Task.Stop.Outcome
	}
	if view.Item.Item.ForwardRevoked {
		return workexecution.Superseded
	}
	return workexecution.Succeeded
}
