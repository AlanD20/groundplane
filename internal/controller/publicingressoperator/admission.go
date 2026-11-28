package publicingressoperator

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	request "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ApplyRoute = "/hosts/{id}/public-ingress/apply"
	ApplyKind  = "public-ingress-apply"
	// The native Apply action belongs to the Blueprint aggregate. Publishing
	// host configuration is an Update with its own work operation.
	TaskAction = workexecution.ActionUpdate
)

// AcceptedApply is the immutable reference used by ingress Task preparation.
// The full, reviewed edge configuration is stored separately in Pending.
type AcceptedApply struct {
	HostID              string `json:"host_id"`
	PublicationRevision int64  `json:"publication_revision"`
	Digest              string `json:"digest"`
}

func (operator *Operator) ApplyPublicIngress(
	ctx context.Context, hostID string, revision int64, body api.PublicIngressApplyRequest, key string,
) (idempotency.IdempotencyResponse, error) {
	if ids.Validate(ids.KindHost, hostID) != nil || revision < 0 || len(body.ReviewToken) != 64 {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Apply requires an exact Host and ingress review",
		)
	}
	desired, err := parseDraft(body.PublicIngressDraft)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	addresses := make([]string, len(desired.Addresses))
	for i, address := range desired.Addresses {
		addresses[i] = address.String()
	}
	canonical := request.CanonicalIntentV1{
		Method: http.MethodPost, Route: ApplyRoute, Scope: request.Scope{Kind: request.ScopePlatform},
		Path: []request.PathBinding{{Name: "id", Value: hostID}}, Query: request.Object(),
		Body: request.JSONBody(request.Object(
			request.Field{Name: "revision", Value: request.String(strconv.FormatInt(revision, 10))},
			request.Field{Name: "review_token", Value: request.String(body.ReviewToken)},
			request.Field{Name: "enabled", Value: request.Bool(desired.Enabled)},
			request.Field{Name: "addresses", Value: intentList(addresses)},
			request.Field{Name: "route_ids", Value: intentList(desired.RouteIDs)},
		)),
	}
	version, digest, err := request.Canonicalize(ctx, canonical)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := operator.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{
		ScopeKind: idempotency.IdempotencyScopePlatform, ScopeID: "-", Method: http.MethodPost, Route: ApplyRoute, Key: key,
	}
	if err := idempotency.ValidateIdempotencyLocator(locator); err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	if prior, found, err := operator.intents.ResolveExisting(ctx, operator.replays, locator, evidence); err != nil {
		return idempotency.IdempotencyResponse{}, err
	} else if found {
		return request.CloneResponse(prior.Response), nil
	}
	now := time.Now().UTC()
	task := workexecution.Task{
		ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor, Action: TaskAction,
		TargetID: hostID, TargetName: "Public ingress", CreatedAt: now, UpdatedAt: now,
		Deadline: now.Add(acceptedaction.QueueTimeout),
	}
	publication, change, err := operator.ingress.PrepareBegin(ctx, hostID, task.ID, revision, body.ReviewToken, desired)
	if err != nil {
		// Concurrent admission may have published this exact replay and changed
		// the ingress revision since the first lookup.
		if kind, _ := errs.KindOf(err); kind == errs.KindStateConflict {
			if prior, found, replayErr := operator.intents.ResolveExisting(ctx, operator.replays, locator, evidence); replayErr != nil {
				return idempotency.IdempotencyResponse{}, replayErr
			} else if found {
				return request.CloneResponse(prior.Response), nil
			}
		}
		return idempotency.IdempotencyResponse{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	protected, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(protected.Ciphertext)
	return acceptedaction.Submit(ctx, operator.submitter, acceptedaction.Submission{
		Task: task, Kind: ApplyKind, Locator: locator, Intent: protected, Evidence: evidence, Publication: change,
	}, AcceptedApply{HostID: hostID, PublicationRevision: publication.Revision, Digest: publication.Digest})
}

func intentList(values []string) request.Value {
	items := make([]request.Value, len(values))
	for i, value := range values {
		items[i] = request.String(value)
	}
	return request.List(items...)
}
