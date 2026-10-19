package servicelifecycle

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *WorkReports) RecoverLifecycle(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceLifecycleRecoveryReportRequest,
) (*agentpb.ServiceLifecycleRecoveryReportAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || len(request.Report) == 0 ||
		len(request.Report) > lifecycleexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle recovery report is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	captured, err := service.records.LoadLifecycleInput(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	input, err := BuildWorkInput(captured)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, result, err := service.records.PrepareRecoveryResult(ctx, view, input, request.Report, at)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(proof.Mutations)
		completion, err := service.queue.ReadCompletionAt(ctx, view)
		if err != nil {
			return nil, err
		}
		completion.Result, completion.At = result, at
		change, err := workqueue.PrepareCompletion(completion)
		if err != nil {
			return nil, err
		}
		defer keyvalue.ZeroMutationBytes(change.Mutations)
		committed, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
		keyvalue.ClearValues(committed.FailureReads)
		if err != nil {
			return nil, err
		}
		if !committed.Succeeded {
			return nil, errs.New(
				errs.KindStateConflict,
				"Service lifecycle recovery ownership changed; resend the saved report",
			)
		}
		view, err = service.reportView(ctx, authorization, request.Attempt)
		if err != nil {
			return nil, err
		}
	}
	saved, completed, err := service.records.LoadRecoveryResult(ctx, view, input)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(saved, request.Report) {
		return nil, errs.New(errs.KindStateConflict, "Service lifecycle recovery replay differs from the saved result")
	}
	attempt := view.Attempt.Attempt
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceLifecycleRecoveryReportAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence:          &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
		BoundaryCompleted: completed, StartedAt: timestamppb.New(*attempt.StartedAt), FinishedAt: timestamppb.New(*attempt.FinishedAt)}, nil
}
