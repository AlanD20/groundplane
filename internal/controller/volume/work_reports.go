package volume

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WorkRecords owns the captured Volume mutation and its atomic resource changes.
// Report handlers cannot settle queue ownership independently of these writes.
type WorkRecords interface {
	LoadVolumeWorkInput(context.Context, workqueue.View) (*agentpb.VolumeWorkInput, error)
	PrepareVolumeWorkCheckpoint(
		context.Context,
		workqueue.ResumeView,
		*agentpb.VolumeWorkInput,
		volumeexecution.Progress,
		time.Time,
	) (workqueue.Change, error)
	PrepareVolumeWorkCompletion(
		context.Context,
		workqueue.ResumeView,
		*agentpb.VolumeWorkInput,
		time.Time,
	) (workqueue.Change, error)
	PrepareFailure(
		context.Context,
		workqueue.ResumeView,
		volumeexecution.FailureReason,
		time.Time,
		bool,
	) (workqueue.Change, workexecution.Completion, error)
	LoadFailure(context.Context, workqueue.ResumeView) ([]byte, error)
	PrepareRecoveryResult(
		context.Context,
		workqueue.ResumeView,
		[]byte,
		time.Time,
	) (workqueue.Change, workexecution.Completion, error)
	LoadRecoveryResult(context.Context, workqueue.ResumeView) ([]byte, bool, error)
}

type WorkReports struct {
	queue   *workqueue.Repository
	records WorkRecords
}

func NewWorkReports(queue *workqueue.Repository, records WorkRecords) (*WorkReports, error) {
	if queue == nil || records == nil {
		return nil, errs.New(errs.KindInternal, "Volume reporting requires execution storage and its publication owner")
	}
	return &WorkReports{queue: queue, records: records}, nil
}

func (service *WorkReports) CheckpointVolume(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.VolumeWorkCheckpointRequest,
) (*agentpb.VolumeWorkCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		len(request.Progress) == 0 || len(request.Progress) > volumeexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Volume checkpoint is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	input, err := service.records.LoadVolumeWorkInput(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errs.New(errs.KindStateConflict, "Volume execution input is unavailable")
	}
	progress, err := volumeexecution.DecodeProgress(
		view.Attempt.Attempt,
		input.Action,
		input.VolumeId,
		request.Progress,
	)
	if err != nil {
		return nil, err
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, input.Action, input.VolumeId)
	if err != nil {
		return nil, err
	}
	change, err := service.records.PrepareVolumeWorkCheckpoint(ctx, view, input, progress, time.Now().UTC())
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
		return nil, errs.New(errs.KindStateConflict, "Volume checkpoint ownership changed; resend the saved report")
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.VolumeWorkCheckpointAck{Attempt: proto.CloneOf(request.Attempt), Sequence: checkpoint.Sequence,
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
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return workqueue.ResumeView{}, err
	}
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Item.Item.Operation != workexecution.Volume || !view.Work.Task.Task.PublicationComplete ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, errs.New(
			errs.KindStateConflict,
			"Volume report has no retained started execution",
		)
	}
	return view, nil
}
