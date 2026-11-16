// Package etcdconfig owns explicit native etcd configuration activation.
package etcdconfig

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/etcdactivation"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const applyRoute = "/etcd/config/apply"

type Document interface {
	Current(context.Context) (string, string, bool, error)
}
type Runtime interface {
	AppliedConfiguration(context.Context) (string, error)
	Apply(context.Context, string, string, string) error
	ObserveApply(context.Context, string, string, string) (etcdactivation.Outcome, error)
	RecoverApply(context.Context, string, string, string) (etcdactivation.Outcome, error)
}
type TaskStore interface {
	PrepareEtcdConfigurationAuthority(
		context.Context,
		etcdconfiguration.Input,
	) (workqueue.Change, error)
}
type Service struct {
	submitter *acceptedaction.Submitter
	document  Document
	runtime   Runtime
	tasks     TaskStore
	evidence  requestidempotency.EvidenceRepository
	intents   *requestidempotency.Coordinator
	inputs    *etcdconfiguration.Repository
	queue     *workqueue.Repository
}

func New(
	document Document,
	runtime Runtime,
	tasks TaskStore,
	inputs *etcdconfiguration.Repository,
	queue *workqueue.Repository,
	evidence requestidempotency.EvidenceRepository,
	intents *requestidempotency.Coordinator,
	submitter *acceptedaction.Submitter,
) (*Service, error) {
	if submitter == nil || document == nil || runtime == nil || tasks == nil || inputs == nil || queue == nil ||
		evidence == nil ||
		intents == nil {
		return nil, errs.New(errs.KindInternal, "etcd config dependencies are required")
	}
	return &Service{
		submitter: submitter,
		document:  document,
		runtime:   runtime,
		tasks:     tasks,
		inputs:    inputs,
		queue:     queue,
		evidence:  evidence,
		intents:   intents,
	}, nil
}

func (s *Service) Apply(
	ctx context.Context,
	expectedRevision string, after []string, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: applyRoute, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)}, requestidempotency.Field{Name: "expected_revision", Value: requestidempotency.String(expectedRevision)})),
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := s.intents.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform,
		ScopeID:   "-",
		Method:    http.MethodPost,
		Route:     applyRoute,
		Key:       key,
	}
	existing, found, err := s.intents.ResolveOperationRootExisting(ctx, s.evidence, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if found {
		return accepted(existing)
	}
	at := time.Now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionApply, TargetID: etcdconfiguration.TargetID(), TargetName: "etcd configuration",
		After: append(
			[]string(nil),
			after...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	return acceptedaction.Submit(ctx, s.submitter, acceptedaction.Submission{
		Task: task, Kind: "etcd-config-apply", Locator: locator, Intent: intent, Evidence: evidence}, applyRequest{ExpectedRevision: expectedRevision})

}

func accepted(result requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if result.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "etcd Apply replay is invalid")
	}
	response := result.Response
	response.Body = append([]byte(nil), response.Body...)
	return response, nil
}
