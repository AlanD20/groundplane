package componentwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/componentwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type WorkReports struct {
	diagnostics *executiondiagnostics.Repository
	files       CapturedFileReader
	ledger      *blueprintunits.Repository
	queue       *workqueue.Repository
	records     *componentrecord.InputRepository
}

func NewWorkReports(
	queue *workqueue.Repository,
	records *componentrecord.InputRepository,
	ledger *blueprintunits.Repository,
	files CapturedFileReader,
	diagnostics *executiondiagnostics.Repository,
) (*WorkReports, error) {
	if queue == nil || records == nil || ledger == nil || files == nil || diagnostics == nil {
		return nil, errs.New(
			errs.KindInternal,
			"Component reporting requires execution storage and its publication owner",
		)
	}
	return &WorkReports{queue: queue, records: records, ledger: ledger, files: files, diagnostics: diagnostics}, nil
}

func (service *WorkReports) CheckpointComponent(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ComponentWorkCheckpointRequest,
) (*agentpb.ComponentWorkCheckpointAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		len(request.Progress) == 0 || len(request.Progress) > componentexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Component checkpoint is missing or invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	view, err := service.reportView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	input, _, _, err := service.records.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	if input == nil {
		return nil, errs.New(errs.KindStateConflict, "Component execution input is unavailable")
	}
	progress, err := componentexecution.DecodeProgress(view.Attempt.Attempt, input, request.Progress)
	if err != nil {
		return nil, err
	}
	checkpoint, err := progress.Checkpoint(view.Attempt.Attempt, input)
	if err != nil {
		return nil, err
	}
	change, err := service.records.PrepareComponentWorkCheckpoint(ctx, view, input, progress, time.Now().UTC())
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
		return nil, errs.New(errs.KindStateConflict, "Component checkpoint ownership changed; resend the saved report")
	}
	service.reportProgress(ctx, view, input, progress)
	digest, err := hex.DecodeString(checkpoint.Evidence.SHA256)
	if err != nil {
		return nil, err
	}
	return &agentpb.ComponentWorkCheckpointAck{Attempt: proto.CloneOf(request.Attempt), Sequence: checkpoint.Sequence,
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
		view.Work.Item.Item.Operation != workexecution.Component || !view.Work.Task.Task.PublicationComplete ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return workqueue.ResumeView{}, errs.New(
			errs.KindStateConflict,
			"Component report has no retained started execution",
		)
	}
	return view, nil
}
