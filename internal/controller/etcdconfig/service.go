// Package etcdconfig owns explicit native etcd configuration activation.
package etcdconfig

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const applyRoute = "/etcd/config/apply"
const inputParam = "etcd_config_input"

type Document interface {
	Current(context.Context) (string, string, bool, error)
}
type Runtime interface {
	AppliedConfiguration(context.Context) (string, error)
	Apply(context.Context, string, string, string) error
}
type TaskStore interface {
	CreateTask(
		context.Context,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}
type Service struct {
	document Document
	runtime  Runtime
	tasks    TaskStore
	evidence requestidempotency.EvidenceRepository
	intents  *requestidempotency.Coordinator
}
type input struct {
	Candidate string `json:"candidate"`
	Previous  string `json:"previous"`
}

func New(
	document Document,
	runtime Runtime,
	tasks TaskStore,
	evidence requestidempotency.EvidenceRepository,
	intents *requestidempotency.Coordinator,
) (*Service, error) {
	if document == nil || runtime == nil || tasks == nil || evidence == nil || intents == nil {
		return nil, errs.New(errs.KindInternal, "etcd config dependencies are required")
	}
	return &Service{document: document, runtime: runtime, tasks: tasks, evidence: evidence, intents: intents}, nil
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
	raw, err := json.Marshal(input{Candidate: candidate, Previous: previous})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	hash := sha256.Sum256(canonical)
	now := time.Now().UTC()
	task := etcd.TaskRecord{
		ID: ids.New(
			ids.KindTask,
		), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan), PlanHash: hex.EncodeToString(hash[:]),
		Owner: taskjournal.PlatformTaskOwner(), Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorController,
		IdempotencyKey: key, RenderGeneration: 1, Type: taskjournal.TaskUpdate, Target: "etcd",
		Params: map[string]string{
			taskjournal.TaskResourceKindParam: taskjournal.TaskResourceEtcd,
			inputParam:                        string(canonical),
		},
		TimeoutSeconds: 120, Status: taskjournal.TaskStatusPending, NextEventSequence: 1, CreatedAt: now, UpdatedAt: now,
	}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: task.ID})
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
	result, err := s.tasks.CreateTask(ctx, task, idempotencyrecord.IdempotencyMarker{
		Kind: idempotencyrecord.IdempotencyMarkerTask, State: idempotencyrecord.IdempotencyMarkerPending,
		Locator: locator, Intent: intent, Response: response, TaskID: task.ID, CreatedAt: now, UpdatedAt: now,
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

type Dispatcher struct {
	service  *Service
	fallback controllertask.Handler
}

func (s *Service) Dispatcher(fallback controllertask.Handler) controllertask.Handler {
	return &Dispatcher{service: s, fallback: fallback}
}
func (d *Dispatcher) Execute(ctx context.Context, task etcd.TaskRecord) error {
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceEtcd {
		return d.fallback.Execute(ctx, task)
	}
	hash := sha256.Sum256([]byte(task.Params[inputParam]))
	if task.Executor != taskjournal.TaskExecutorController || task.Owner != taskjournal.PlatformTaskOwner() ||
		task.Actor != taskjournal.TaskActorOperator || task.Type != taskjournal.TaskUpdate || task.Target != "etcd" ||
		task.TimeoutSeconds != 120 || task.RenderGeneration != 1 || len(task.Steps) != 0 || len(task.Materializations) != 0 ||
		len(task.Params) != 2 || task.PlanHash != hex.EncodeToString(hash[:]) {
		return errs.New(errs.KindValidationFailed, "etcd Apply Task authority is invalid")
	}
	frozen, err := jcs.Decode[input]([]byte(task.Params[inputParam]))
	if err != nil {
		return err
	}
	for _, doc := range []string{frozen.Candidate, frozen.Previous} {
		if err := config.ValidateEtcdDocument(ctx, []byte(doc)); err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	return d.service.runtime.Apply(ctx, task.ID, frozen.Candidate, frozen.Previous)
}
