package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
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
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type localAgentConfigIdempotency interface {
	Prepare(context.Context, string, localagent.Config) (localAgentConfigEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		localAgentConfigEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		localAgentConfigEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		localAgentConfigEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableLocalAgentConfigIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableLocalAgentConfigIdempotency(
	coordinator *idempotentintent.Coordinator,
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
	labelFields := make([]idempotentintent.Field, 0, len(labelKeys))
	for _, key := range labelKeys {
		labelFields = append(labelFields, idempotentintent.Field{
			Name: key, Value: idempotentintent.String(config.Labels[key]),
		})
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPut,
		Route:  localAgentConfigRoute,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopePlatform},
		Path:   []idempotentintent.PathBinding{{Name: "id", Value: agentID}},
		Query:  idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{
				Name: "pull_interval_seconds", Value: idempotentintent.Integer(int64(config.PullIntervalSeconds)),
			},
			idempotentintent.Field{
				Name: "max_concurrent_tasks", Value: idempotentintent.Integer(int64(config.MaxConcurrentTasks)),
			},
			idempotentintent.Field{Name: "labels", Value: idempotentintent.Object(labelFields...)},
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
	locator etcd.IdempotencyLocator,
	evidence localAgentConfigEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableLocalAgentConfigIdempotency) ResolveKnown(
	ctx context.Context,
	evidence localAgentConfigEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableLocalAgentConfigIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence localAgentConfigEvidence,
	original error,
) (idempotentintent.Resolution, error) {
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
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopePlatform,
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
		if resolution.Kind != idempotentintent.ResolutionReplay {
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
	if current.Record.Phase == etcd.LocalAgentPhaseDeleting {
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
	response := etcd.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, adapter.now().UTC())
	if err != nil {
		return localagent.ConfigUpdateResult{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	updated, transaction, mutationErr := adapter.repository.UpdateConfigIdempotent(
		ctx,
		current,
		etcd.LocalAgentConfig{
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
	case idempotentintent.ResolutionApplied:
		stored, err := localAgentRecordFromDurable(updated)
		if err != nil {
			return localagent.ConfigUpdateResult{}, err
		}
		return localagent.ConfigUpdateResult{
			Applied: true, Stored: stored, ResponseBody: append([]byte(nil), responseBody...),
		}, nil
	case idempotentintent.ResolutionReplay:
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
