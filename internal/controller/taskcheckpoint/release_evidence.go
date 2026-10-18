package taskcheckpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/executionplan"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *ReleaseCheckpointService) PublishReleaseEffect(ctx context.Context, authorization agentregistration.WorkAuthorization, request *agentpb.ReleaseEffectEvidenceRequest) (*agentpb.ReleaseEffectEvidenceAck, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || request.Evidence == nil || len(request.Evidence.Sha256) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "Release effect evidence request is invalid")
	}
	reference := workexecution.Input{ID: request.Evidence.InputId, SHA256: hex.EncodeToString(request.Evidence.Sha256)}
	if err := service.RecordReleaseEffect(ctx, authorization, request.Attempt, request.Record, reference); err != nil {
		return nil, err
	}
	return &agentpb.ReleaseEffectEvidenceAck{Attempt: proto.CloneOf(request.Attempt), Evidence: proto.CloneOf(request.Evidence)}, nil
}

// RecordReleaseEffect retains an already-authorized effect's proof. It neither
// grants a new mutation nor advances the execution cursor; Confirm consumes the
// immutable evidence in its own guarded transaction.
func (service *ReleaseCheckpointService) RecordReleaseEffect(ctx context.Context, authorization agentregistration.WorkAuthorization, identity *agentpb.WorkAttemptIdentity, raw []byte, reference workexecution.Input) error {
	if service == nil || ctx == nil || len(raw) == 0 || len(raw) > releaseexecution.MaximumCheckpointBytes || reference.Validate() != nil {
		return errs.New(errs.KindValidationFailed, "Release effect evidence is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return err
	}
	view, err := service.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authorization.Identity())
	if err != nil {
		return err
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(view.Attempt.Attempt, claimed) {
		return errs.New(errs.KindStateConflict, "Release evidence does not belong to the active executor")
	}
	owner := service.owners[view.Work.Item.Item.Operation]
	if owner == nil {
		return errs.New(errs.KindStateConflict, "Release evidence owner is unavailable")
	}
	var change workqueue.Change
	if claimed.Direction == workexecution.Recovery {
		input, loadErr := owner.LoadRecoveryInput(ctx, view)
		if loadErr != nil {
			return loadErr
		}
		change, err = releasework.PrepareRecoveryEffect(ctx, service.store, view, input, raw, reference)
	} else {
		input, loadErr := owner.LoadCheckpointInput(ctx, view)
		if loadErr != nil {
			return loadErr
		}
		change, err = releasework.PrepareEffectEvidence(ctx, service.store, view, input, raw, reference)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := service.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Release evidence authority changed; reload the saved attempt")
	}
	return nil
}
