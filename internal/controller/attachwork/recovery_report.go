package attachwork

import (
	"bytes"
	"context"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
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

func (service *Reports) ReportProvisionRecovery(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachProvisionRecoveryReportRequest,
) (*agentpb.AttachProvisionRecoveryReportAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil ||
		len(request.Report) == 0 || len(request.Report) > attachexecution.MaximumProvisionRecoveryBytes {
		return nil, errs.New(errs.KindValidationFailed, "Attach inspection report is missing or invalid")
	}
	evidence := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	view, guards, err := service.readDirectionalView(ctx, authorization, request.Attempt, workexecution.Recovery)
	if err != nil {
		return nil, err
	}
	if view.Attempt.Attempt.FinishedAt == nil {
		at := time.Now().UTC()
		proof, result, err := service.inputs.repository.PrepareRecoveryResult(ctx, view, request.Report, evidence, at, false)
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
			return nil, errs.New(errs.KindStateConflict, "Attach inspection ownership changed; resend the saved report")
		}
		view, _, err = service.readDirectionalView(ctx, authorization, request.Attempt, workexecution.Recovery)
		if err != nil {
			return nil, err
		}
	}
	saved, err := service.inputs.repository.LoadRecoveryResult(ctx, view)
	if err != nil {
		return nil, err
	}
	attempt := view.Attempt.Attempt
	if !bytes.Equal(saved, request.Report) || attempt.Result == nil || *attempt.Result != evidence {
		return nil, errs.New(errs.KindStateConflict, "Attach inspection replay differs from the saved result")
	}
	input, _, err := service.inputs.Recovery(ctx, view.Work, attempt)
	if err == nil {
		var inspection attachexecution.ProvisionInspection
		inspection, err = attachexecution.DecodeProvisionInspection(input, saved, evidence)
		if err == nil {
			service.reportInspection(ctx, attempt, inspection)
		}
	}
	if err != nil {
		slog.WarnContext(ctx, "Attach inspection diagnostics incomplete", "attempt_id", attempt.ID, "error", err)
	}
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.AttachProvisionRecoveryReportAck{Attempt: proto.CloneOf(request.Attempt),
		Evidence: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest}, Outcome: string(attempt.Outcome),
		FinishedAt: timestamppb.New(*attempt.FinishedAt)}
	if attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	return ack, nil
}
