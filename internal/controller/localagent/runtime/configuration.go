package runtime

import (
	"context"
	"encoding/json"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"net/http"
	"sort"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	localAgentConfigRoute           = "/agents/{id}/config"
	maximumAgentConfigWriteAttempts = 3
)

type localAgentConfigEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type localAgentConfigIdempotency interface {
	Prepare(context.Context, string, localagent.Config) (localAgentConfigEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		localAgentConfigEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		localAgentConfigEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		localAgentConfigEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableLocalAgentConfigIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewConfigIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableLocalAgentConfigIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Agent config idempotency is not configured")
	}
	return &durableLocalAgentConfigIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableLocalAgentConfigIdempotency) Prepare(
	ctx context.Context,
	agentID string,
	config localagent.Config,
) (localAgentConfigEvidence, error) {
	labelKeys := make([]string, 0, len(config.Labels))
	for key := range config.Labels {
		labelKeys = append(labelKeys, key)
	}
	sort.Strings(labelKeys)
	labelFields := make([]requestidempotency.Field, 0, len(labelKeys))
	for _, key := range labelKeys {
		labelFields = append(labelFields, requestidempotency.Field{
			Name: key, Value: requestidempotency.String(config.Labels[key]),
		})
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPut,
		Route:  localAgentConfigRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopePlatform},
		Path:   []requestidempotency.PathBinding{{Name: "id", Value: agentID}},
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{
				Name: "pull_interval_seconds", Value: requestidempotency.Integer(int64(config.PullIntervalSeconds)),
			},
			requestidempotency.Field{
				Name: "max_concurrent_tasks", Value: requestidempotency.Integer(int64(config.MaxConcurrentTasks)),
			},
			requestidempotency.Field{Name: "labels", Value: requestidempotency.Object(labelFields...)},
		)),
	})
	if err != nil {
		return localAgentConfigEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return localAgentConfigEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return localAgentConfigEvidence{}, err
	}
	return localAgentConfigEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableLocalAgentConfigIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence localAgentConfigEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableLocalAgentConfigIdempotency) ResolveKnown(
	ctx context.Context,
	evidence localAgentConfigEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableLocalAgentConfigIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence localAgentConfigEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func (adapter *localAgentRepositoryAdapter) UpdateConfig(
	ctx context.Context,
	agentID string,
	config localagent.Config,
	idempotencyKey string,
) (localagent.ConfigUpdateResult, error) {
	for attempt := 0; attempt < maximumAgentConfigWriteAttempts; attempt++ {
		result, err := adapter.updateConfigOnce(ctx, agentID, config, idempotencyKey)
		if err == nil {
			return result, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumAgentConfigWriteAttempts-1 {
			return localagent.ConfigUpdateResult{}, err
		}
	}
	return localagent.ConfigUpdateResult{}, errs.New(
		errs.KindInternal,
		"Agent config replacement retry bound was not enforced",
	)
}

func (adapter *localAgentRepositoryAdapter) updateConfigOnce(
	ctx context.Context,
	agentID string,
	config localagent.Config,
	idempotencyKey string,
) (localagent.ConfigUpdateResult, error) {
	evidence, err := adapter.idempotency.Prepare(ctx, agentID, config)
	if err != nil {
		return localagent.ConfigUpdateResult{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopePlatform,
		ScopeID:   "-",
		Method:    http.MethodPut,
		Route:     localAgentConfigRoute,
		Key:       idempotencyKey,
	}
	resolution, found, err := adapter.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return localagent.ConfigUpdateResult{}, err
	}
	if found {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return localagent.ConfigUpdateResult{}, errs.New(
				errs.KindInternal,
				"Agent config replay resolution is invalid",
			)
		}
		return localagent.ConfigUpdateResult{
			ResponseBody: append([]byte(nil), resolution.Response.Body...),
		}, nil
	}
	current, err := adapter.repository.GetSingleton(ctx)
	if err != nil {
		return localagent.ConfigUpdateResult{}, err
	}
	if current.Record.ID != agentID {
		return localagent.ConfigUpdateResult{}, errs.New(errs.KindAgentNotFound, "Agent was not found")
	}
	if current.Record.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return localagent.ConfigUpdateResult{}, errs.New(
			errs.KindStateConflict,
			"deleting local Agent config cannot be changed",
		)
	}
	responseBody, err := json.Marshal(apiTypes.AgentConfig{
		PullIntervalSeconds: int(config.PullIntervalSeconds),
		MaxConcurrentTasks:  int(config.MaxConcurrentTasks),
		Labels:              cloneLocalAgentLabels(config.Labels),
	})
	if err != nil {
		return localagent.ConfigUpdateResult{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, adapter.now().UTC())
	if err != nil {
		return localagent.ConfigUpdateResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	updated, transaction, mutationErr := adapter.repository.UpdateConfigIdempotent(
		ctx,
		current,
		localagentrecord.LocalAgentConfig{
			PullIntervalSeconds: config.PullIntervalSeconds,
			MaxConcurrentTasks:  config.MaxConcurrentTasks,
			Labels:              cloneLocalAgentLabels(config.Labels),
		},
		marker,
	)
	if mutationErr != nil {
		if !isUnknownAgentConfigOutcome(mutationErr) {
			return localagent.ConfigUpdateResult{}, mutationErr
		}
		resolution, err = adapter.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = adapter.idempotency.ResolveKnown(ctx, evidence, transaction)
	}
	if err != nil {
		return localagent.ConfigUpdateResult{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		stored, err := localAgentRecordFromDurable(updated)
		if err != nil {
			return localagent.ConfigUpdateResult{}, err
		}
		return localagent.ConfigUpdateResult{
			Applied: true, Stored: stored, ResponseBody: append([]byte(nil), responseBody...),
		}, nil
	case requestidempotency.ResolutionReplay:
		return localagent.ConfigUpdateResult{
			ResponseBody: append([]byte(nil), resolution.Response.Body...),
		}, nil
	default:
		return localagent.ConfigUpdateResult{}, errs.New(
			errs.KindInternal,
			"Agent config resolution is invalid",
		)
	}
}

func isUnknownAgentConfigOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
