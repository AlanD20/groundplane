// Package etcdconfig owns explicit native etcd configuration activation.
package etcdconfig

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/etcdactivation"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
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
	CreateEtcdConfigurationWork(
		context.Context,
		etcdconfiguration.Input,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}
type Service struct {
	document Document
	runtime  Runtime
	tasks    TaskStore
	evidence requestidempotency.EvidenceRepository
	intents  *requestidempotency.Coordinator
	inputs   *etcdconfiguration.Repository
	queue    *workqueue.Repository
}

func New(
	document Document,
	runtime Runtime,
	tasks TaskStore,
	inputs *etcdconfiguration.Repository,
	queue *workqueue.Repository,
	evidence requestidempotency.EvidenceRepository,
	intents *requestidempotency.Coordinator,
) (*Service, error) {
	if document == nil || runtime == nil || tasks == nil || inputs == nil || queue == nil || evidence == nil || intents == nil {
		return nil, errs.New(errs.KindInternal, "etcd config dependencies are required")
	}
	return &Service{document: document, runtime: runtime, tasks: tasks, inputs: inputs, queue: queue, evidence: evidence, intents: intents}, nil
}

func (s *Service) Apply(
	ctx context.Context,
	expectedRevision, key string,
) (idempotencyrecord.IdempotencyResponse, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: applyRoute, Scope: requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Query: requestidempotency.Object(), Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "expected_revision", Value: requestidempotency.String(expectedRevision)})),
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
	candidate, revision, _, err := s.document.Current(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if revision != expectedRevision {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"etcd config changed; reload before applying",
		)
	}
	previous, err := s.runtime.AppliedConfiguration(ctx)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for _, document := range []string{candidate, previous} {
		if err := config.ValidateEtcdDocument(ctx, []byte(document)); err != nil {
			return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	now := time.Now().UTC()
	captured := etcdconfiguration.Input{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), SelectedRevision: revision, Candidate: candidate, Previous: previous, CreatedAt: now}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: captured.TaskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	result, err := s.tasks.CreateEtcdConfigurationWork(ctx, captured, idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: intent, Response: response, TaskID: captured.TaskID, CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		read, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		replay, found, readErr := s.intents.ResolveOperationRootExisting(read, s.evidence, locator, evidence)
		if readErr != nil || !found {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		return accepted(replay)
	}
	outcome, marker, conflict, err := result.Classify()
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch outcome {
	case etcd.IdempotencyKnownApplied:
		return response, nil
	case etcd.IdempotencyKnownConflict:
		return idempotencyrecord.IdempotencyResponse{}, conflict
	case etcd.IdempotencyKnownExisting:
		replay, err := s.intents.ResolveMarker(ctx, evidence, marker)
		if errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) {
			return marker.Response, nil
		}
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
		return accepted(replay)
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"etcd Apply publication outcome is invalid",
		)
	}
}

func accepted(result requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if result.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "etcd Apply replay is invalid")
	}
	response := result.Response
	response.Body = append([]byte(nil), response.Body...)
	return response, nil
}
