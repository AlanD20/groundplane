package servicelifecycle

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// WorkRecords owns the captured Service lifecycle mutation and its atomic resource changes.
// Report handlers cannot settle queue ownership independently of these writes.
type WorkRecords interface {
	PrepareRecoveryResult(
		context.Context,
		workqueue.ResumeView,
		*agentpb.ServiceLifecycleWorkInput,
		[]byte,
		time.Time,
	) (workqueue.Change, workexecution.Completion, error)
	LoadRecoveryResult(context.Context, workqueue.ResumeView, *agentpb.ServiceLifecycleWorkInput) ([]byte, bool, error)
	LoadHookInputs(
		context.Context,
		workqueue.View,
	) (servicelifecyclework.Input, *taskconfiguration.BackingHookEncryptedInputs, []keyvalue.Condition, error)
	PrepareHookDelivery(
		context.Context,
		workqueue.ResumeView,
		*agentpb.ServiceLifecycleWorkInput,
		string,
		time.Time,
	) (workqueue.Change, error)
	PrepareLifecycleFailure(
		context.Context,
		workqueue.ResumeView,
		servicelifecyclework.Input,
		*agentpb.ServiceLifecycleWorkInput,
		lifecycleexecution.FailureReason,
		time.Time,
		bool,
	) (workqueue.Change, workexecution.Completion, error)
	LoadLifecycleFailure(context.Context, workqueue.ResumeView, *agentpb.ServiceLifecycleWorkInput) ([]byte, error)
	PrepareLifecycleCompletion(
		context.Context,
		workqueue.ResumeView,
		servicelifecyclework.Input,
		*agentpb.ServiceLifecycleWorkInput,
		time.Time,
	) (workqueue.Change, error)
	LoadLifecycleInput(context.Context, workqueue.View) (servicelifecyclework.Input, error)
	PrepareLifecycleCheckpoint(
		context.Context,
		workqueue.ResumeView,
		servicelifecyclework.Input,
		*agentpb.ServiceLifecycleWorkInput,
		lifecycleexecution.Progress,
		time.Time,
	) (workqueue.Change, error)
}

type WorkReports struct {
	queue   *workqueue.Repository
	records WorkRecords
	hooks   LifecycleHookResolver
}

func NewWorkReports(
	queue *workqueue.Repository,
	records WorkRecords,
	hooks LifecycleHookResolver,
) (*WorkReports, error) {
	if queue == nil || records == nil || hooks == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Service lifecycle reporting requires execution storage and its publication owner",
		)
	}
	return &WorkReports{queue: queue, records: records, hooks: hooks}, nil
}

func (service *WorkReports) CheckpointLifecycle(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceLifecycleCheckpointRequest,
) (*agentpb.ServiceLifecycleCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		len(request.Progress) == 0 || len(request.Progress) > lifecycleexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Service lifecycle checkpoint is missing or invalid")
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
	progress, err := lifecycleexecution.DecodeProgress(view.Attempt.Attempt, input, request.Progress)
	if err != nil {
		return nil, err
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, input)
	if err != nil {
		return nil, err
	}
	change, err := service.records.PrepareLifecycleCheckpoint(ctx, view, captured, input, progress, time.Now().UTC())
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
		return nil, errs.New(
			errs.KindStateConflict,
			"Service lifecycle checkpoint ownership changed; resend the saved report",
		)
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceLifecycleCheckpointAck{
		Attempt:    proto.CloneOf(request.Attempt),
		Sequence:   checkpoint.Sequence,
		StageId:    checkpoint.StageID,
		Evidence:   &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
		RecordedAt: timestamppb.New(checkpoint.RecordedAt),
	}, nil
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
		view.Work.Item.Item.Operation != workexecution.ServiceLifecycle || !view.Work.Task.Task.PublicationComplete ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, errs.New(
			errs.KindStateConflict,
			"Service lifecycle report has no retained started execution",
		)
	}
	return view, nil
}
