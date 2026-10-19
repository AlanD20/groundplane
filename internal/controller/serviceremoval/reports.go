package serviceremoval

import (
	"context"
	"encoding/hex"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkRecords interface {
	SettleRecoveredServiceRemoval(
		context.Context,
		workqueue.ResumeView,
		*agentpb.ServiceLifecycleWorkInput,
		[]keyvalue.Condition,
		time.Time,
	) error
	PrepareServiceRemovalCompletion(
		context.Context,
		workqueue.ResumeView,
		*agentpb.ServiceLifecycleWorkInput,
		time.Time,
	) (workqueue.Change, error)
	LoadServiceRemovalRecoveryConfiguration(
		context.Context,
		workqueue.ResumeView,
		*agentpb.ServiceLifecycleWorkInput,
	) (*runtimeconfiguration.Reference, []taskmaterialization.Record, []keyvalue.Condition, error)
	AuthorizeServiceRemovalConfiguration(context.Context, []keyvalue.Condition) error
}

type WorkReports struct {
	queue   *workqueue.Repository
	inputs  *serviceremovalwork.Repository
	records WorkRecords
	files   CapturedFileReader
}

type CapturedFileReader interface {
	OpenCapturedFile(
		context.Context,
		string,
		string,
		uint64,
		taskmaterialization.Record,
	) (entrymaterialization.Header, io.ReadCloser, error)
}

func NewWorkReports(
	queue *workqueue.Repository,
	inputs *serviceremovalwork.Repository,
	records WorkRecords,
	files CapturedFileReader,
) (*WorkReports, error) {
	if queue == nil || inputs == nil || records == nil || files == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Service removal reporting requires execution, input and resource settlement storage",
		)
	}
	return &WorkReports{queue: queue, inputs: inputs, records: records, files: files}, nil
}

func (service *WorkReports) CheckpointRemovalRecovery(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceRemovalRecoveryCheckpointRequest,
) (*agentpb.ServiceRemovalRecoveryCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		len(request.Progress) == 0 || len(request.Progress) > removalexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Service removal recovery checkpoint is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.recoveryView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	captured, _, err := service.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	input, err := BuildHostInput(captured)
	if err != nil {
		return nil, err
	}
	saved, err := service.inputs.LoadRecovery(ctx, view.Work, input)
	if err != nil {
		return nil, err
	}
	progress, err := removalexecution.DecodeProgress(view.Attempt.Attempt, input, saved.Capture, request.Progress)
	if err != nil {
		return nil, err
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, input, saved.Capture)
	if err != nil {
		return nil, err
	}
	change, err := service.inputs.PrepareRecoveryCheckpoint(ctx, view, input, progress, time.Now().UTC())
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
		return nil, errs.New(
			errs.KindStateConflict,
			"Service removal recovery ownership changed; resend the saved checkpoint",
		)
	}
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ServiceRemovalRecoveryCheckpointAck{Attempt: proto.CloneOf(request.Attempt),
		Sequence: checkpoint.Sequence, StageId: checkpoint.StageID,
		Evidence:   &agentpb.WorkInputReference{InputId: checkpoint.Evidence.ID, Sha256: digest},
		RecordedAt: timestamppb.New(checkpoint.RecordedAt)}, nil
}

func (service *WorkReports) recoveryView(ctx context.Context, authorization agentregistration.WorkAuthorization,
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
	if !workexecution.SameAssignment(claimed, view.Attempt.Attempt) || claimed.Direction != workexecution.Recovery ||
		view.Attempt.Attempt.StartedAt == nil || view.Work.Item.Item.Operation != workexecution.ServiceRemoval ||
		!view.Work.Task.Task.PublicationComplete || view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, errs.New(
			errs.KindStateConflict,
			"Service removal recovery report has no retained started execution",
		)
	}
	return view, nil
}
