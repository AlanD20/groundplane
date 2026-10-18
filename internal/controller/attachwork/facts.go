package attachwork

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *Reports) StoreAttachProvisionFacts(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.AttachProvisionFactsRequest,
) (*agentpb.AttachProvisionFactsAck, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil || proto.Size(request) > attachexecution.MaximumPlanBytes {
		return nil, errs.New(errs.KindValidationFailed, "Attach fact publication is malformed or oversized")
	}
	defer attachexecution.ClearProvisionFacts(request)
	view, guards, err := service.readView(ctx, authorization, request.Attempt)
	if err != nil {
		return nil, err
	}
	input, err := service.inputs.Forward(ctx, view)
	if err != nil {
		return nil, err
	}
	defer attachexecution.ClearProvision(input.Provision)
	output, digest, err := attachexecution.DecodeProvisionFacts(input, request)
	if err != nil {
		return nil, err
	}
	defer output.Clear()
	source, err := service.inputs.repository.LoadCaptured(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer source.Clear()
	sealed, err := service.facts.SealCapturedProvisionFacts(ctx, source.Generation.Record, output)
	if err != nil {
		return nil, err
	}
	defer clear(sealed.Ciphertext)
	attempt := view.Attempt.Attempt
	record := blueprintattachoutputs.Generation{EnvironmentID: view.Work.Task.Task.Scope.EnvironmentID,
		TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID, Executor: attempt.Executor,
		Input: attempt.Input, AttachID: source.Generation.Record.AttachID, OperationID: source.Generation.Record.OperationID,
		ResultSHA256: hex.EncodeToString(digest), Facts: sealed, CreatedAt: time.Now().UTC()}
	change, err := service.inputs.repository.PrepareFacts(ctx, view, input, record)
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
		return nil, errs.New(errs.KindStateConflict, "Attach fact publication ownership changed; resend the saved output")
	}
	return &agentpb.AttachProvisionFactsAck{Attempt: proto.CloneOf(request.Attempt), ResultSha256: digest}, nil
}
