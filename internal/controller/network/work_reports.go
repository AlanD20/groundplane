package network

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WorkRecords owns the captured Network mutation and its atomic resource changes.
// Report handlers cannot settle queue ownership independently of these writes.
type WorkRecords interface {
	LoadNetworkWorkInput(context.Context, workqueue.View) (*agentpb.NetworkWorkInput, error)
	PrepareNetworkWorkCheckpoint(context.Context, workqueue.ResumeView, *agentpb.NetworkWorkInput, networkexecution.Progress, time.Time) (workqueue.Change, error)
	PrepareNetworkWorkCompletion(context.Context, workqueue.ResumeView, *agentpb.NetworkWorkInput, time.Time) (workqueue.Change, error)
	PrepareFailure(context.Context, workqueue.ResumeView, networkexecution.FailureReason, time.Time, bool) (workqueue.Change, workexecution.Completion, error)
	LoadFailure(context.Context, workqueue.ResumeView) ([]byte, error)
	PrepareRecoveryResult(context.Context, workqueue.ResumeView, []byte, time.Time) (workqueue.Change, workexecution.Completion, error)
	LoadRecoveryResult(context.Context, workqueue.ResumeView) ([]byte, bool, error)
}

type WorkReports struct {
	queue   *workqueue.Repository
	records WorkRecords
}

func NewWorkReports(queue *workqueue.Repository, records WorkRecords) (*WorkReports, error) {
	if queue == nil || records == nil {
		return nil, errs.New(errs.KindInternal, "Network reporting requires execution storage and its publication owner")
	}
	return &WorkReports{queue: queue, records: records}, nil
}

func (service *WorkReports) CheckpointNetwork(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.NetworkWorkCheckpointRequest,
) (*agentpb.NetworkWorkCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		len(request.Progress) == 0 || len(request.Progress) > networkexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Network checkpoint is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	input, err := service.records.LoadNetworkWorkInput(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errs.New(errs.KindStateConflict, "Network execution input is unavailable")
	}
	progress, err := networkexecution.DecodeProgress(view.Attempt.Attempt, input.Action, input.NetworkId, request.Progress)
	if err != nil {
		return nil, err
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, input.Action, input.NetworkId)
	if err != nil {
		return nil, err
	}
	change, err := service.records.PrepareNetworkWorkCheckpoint(ctx, view, input, progress, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Network checkpoint ownership changed; resend the saved report")
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.NetworkWorkCheckpointAck{Attempt: proto.CloneOf(request.Attempt), Sequence: checkpoint.Sequence,
		StageId: checkpoint.StageID, Evidence: &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
		RecordedAt: timestamppb.New(checkpoint.RecordedAt)}, nil
}

func (service *WorkReports) reportView(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity,
) (workqueue.ResumeView, error) {
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, err
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Item.Item.Operation != workexecution.Network || !view.Work.Task.Task.PublicationComplete ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, errs.New(errs.KindStateConflict, "Network report has no retained started execution")
	}
	return view, nil
}
