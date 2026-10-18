package backingservices

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *WorkReports) CompleteBackingProvision(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackingProvisionCompletionRequest,
) (*agentpb.BackingProvisionCompletionAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil {
		return nil, errs.New(errs.KindValidationFailed, "backing creation completion evidence is required")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt, true)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.Direction != workexecution.Forward || view.Checkpoint.Record == nil {
		return nil, errs.New(errs.KindStateConflict, "backing creation completion has no final forward checkpoint")
	}
	observation, err := backingprovision.DecodeRuntimeObservation(request.RuntimeObservation)
	if err != nil {
		return nil, err
	}
	evidence, err := backingprovision.CompletionEvidence(view.Attempt.Attempt, *view.Checkpoint.Record, observation)
	if err != nil {
		return nil, err
	}
	claimed := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	if claimed != evidence {
		return nil, errs.New(errs.KindStateConflict, "backing creation completion names different runtime evidence")
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		if observation.ObservedAt.After(at.Add(30 * time.Second)) {
			return nil, errs.New(errs.KindValidationFailed, "backing creation observation is ahead of the Controller clock")
		}
		if observation.ObservedAt.After(at) {
			at = observation.ObservedAt
		}
		change, _, err := service.records.PrepareCompleted(ctx, view, observation, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ClearMutationValues(change.Mutations)
		change.Conditions = append(change.Conditions, guards...)
		committed, err := service.queue.Commit(ctx, change)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(errs.KindStateConflict, "backing creation completion ownership changed; resend the saved result")
		}
		view, err = service.queue.ReadResume(ctx, view.Attempt.Attempt.TaskID, view.Attempt.Attempt.WorkItemID, view.Attempt.Attempt.ID, authorization.Identity())
		if err != nil {
			return nil, err
		}
	}
	attempt := view.Attempt.Attempt
	if attempt.Result == nil || *attempt.Result != evidence || attempt.FinishedAt == nil || attempt.StartedAt == nil ||
		attempt.Outcome != workexecution.Succeeded && attempt.Outcome != workexecution.Cancelled && attempt.Outcome != workexecution.Superseded {
		return nil, errs.New(errs.KindStateConflict, "backing creation completion replay differs from its saved result")
	}
	encoded, err := encodeBackingReference(evidence)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackingProvisionCompletionAck{Attempt: proto.CloneOf(request.Attempt), Evidence: encoded,
		Outcome: string(attempt.Outcome), StartedAt: timestamppb.New(*attempt.StartedAt), FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
