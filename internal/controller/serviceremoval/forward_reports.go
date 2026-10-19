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

// Forward removal uses the member-execution protocol, but its reports belong
// to the removal owner, not lifecycle resource settlement.
func (service *WorkReports) CheckpointLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceLifecycleCheckpointRequest,
) (*agentpb.ServiceLifecycleCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		len(request.Progress) == 0 || len(request.Progress) > lifecycleexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Service removal checkpoint is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, input, err := service.forwardReport(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	progress, err := lifecycleexecution.DecodeProgress(view.Attempt.Attempt, input, request.Progress)
	if err != nil {
		return nil, err
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, input)
	if err != nil {
		return nil, err
	}
	change, err := service.inputs.PrepareCheckpoint(ctx, view, input, progress, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Service removal ownership changed; resend the saved checkpoint")
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceLifecycleCheckpointAck{Attempt: proto.CloneOf(request.Attempt),
		Sequence: checkpoint.Sequence, StageId: checkpoint.StageID,
		Evidence:   &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
		RecordedAt: timestamppb.New(checkpoint.RecordedAt)}, nil
}

func (service *WorkReports) forwardReport(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity,
) (workqueue.ResumeView, *agentpb.ServiceLifecycleWorkInput, error) {
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, nil, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return workqueue.ResumeView{}, nil, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || claimed.Direction != workexecution.Forward ||
		view.Attempt.Attempt.StartedAt == nil || view.Work.Item.Item.Operation != workexecution.ServiceRemoval ||
		!view.Work.Task.Task.PublicationComplete || view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, nil, errs.New(
			errs.KindStateConflict,
			"Service removal report has no retained started execution",
		)
	}
	captured, _, err := service.inputs.Load(ctx, view.Work)
	if err != nil {
		return workqueue.ResumeView{}, nil, err
	}
	input, err := BuildHostInput(captured)
	return view, input, err
}
