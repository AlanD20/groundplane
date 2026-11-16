package acceptedaction

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Submission struct {
	SecretIDs    []string
	ReplayTarget *idempotency.IdempotencyReplayTarget
	Task         workexecution.Task
	Kind         string
	Locator      idempotency.IdempotencyLocator
	Intent       idempotency.ProtectedIntentRecord
	Evidence     requestidempotency.ProtectedEvidence
}

// Submitter owns acceptance storage, encryption and replay as one boundary.
// Action owners supply typed operator input and never assemble partial commits.
type Submitter struct {
	payloads    *Payloads
	queue       *workqueue.Repository
	replay      *etcd.IdempotencyRepository
	coordinator *requestidempotency.Coordinator
}

func NewSubmitter(payloads *Payloads, queue *workqueue.Repository,
	replay *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
) (*Submitter, error) {
	if payloads == nil || queue == nil || replay == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "accepted action submission dependencies are required")
	}
	return &Submitter{payloads: payloads, queue: queue, replay: replay, coordinator: coordinator}, nil
}

func Submit[T any](ctx context.Context, submitter *Submitter, submission Submission, input T,
) (acceptedResponse idempotency.IdempotencyResponse, failure error) {
	if submitter == nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"accepted action submission dependencies are required",
		)
	}
	reference, payload, err := Seal(ctx, submitter.payloads, submission.Task.ID, submission.Kind, input)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer keyvalue.ClearMutationValues(payload.Mutations)
	task := submission.Task
	task.AcceptedIntent = &reference
	credentials, err := submitter.queue.PrepareCredentialReferences(ctx, task, submission.SecretIDs)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer keyvalue.ClearMutationValues(credentials.Mutations)
	applied := false
	if len(submission.SecretIDs) != 0 {
		defer func() {
			bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer cancel()
			if cleanup := submitter.queue.FinishCredentialReferences(bounded, task.ID, applied); cleanup != nil {
				failure = errs.WrapJoined(errs.KindInternal, failure, cleanup)
			}
		}()
	}
	body, err := json.Marshal(api.TaskAccepted{TaskID: task.ID})
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotency.IdempotencyResponse{Status: 202, ContentKind: "application/json", Body: body}
	var replayTarget *idempotency.IdempotencyReplayTarget
	if submission.ReplayTarget != nil {
		value := *submission.ReplayTarget
		replayTarget = &value
	}
	marker := idempotency.IdempotencyMarker{
		ReplayTarget: replayTarget,
		Kind:         idempotency.IdempotencyMarkerTask,
		State:        idempotency.IdempotencyMarkerPending,
		Locator:      submission.Locator,
		Intent:       submission.Intent,
		Response:     response,
		TaskID:       task.ID,
		CreatedAt:    task.CreatedAt,
		UpdatedAt:    task.CreatedAt,
	}
	accepted, err := submitter.queue.PrepareAcceptedTask(ctx, task, marker)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer keyvalue.ClearMutationValues(accepted.Mutations)
	result, err := submitter.replay.PublishAcceptedAction(ctx, marker, accepted, payload, credentials)
	var resolution requestidempotency.Resolution
	if err != nil {
		resolution, err = submitter.coordinator.ResolveUnknown(
			ctx,
			submitter.replay,
			submission.Locator,
			submission.Evidence,
			err,
		)
	} else {
		resolution, err = submitter.coordinator.ResolveKnown(ctx, submission.Evidence, result)
	}
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		applied = true
		return response, nil
	case requestidempotency.ResolutionReplay:
		return resolution.Response, nil
	default:
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"accepted action replay outcome is invalid",
		)
	}
}
