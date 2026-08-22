package app

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeLocalAgentConfigIdempotency struct {
	resolution idempotentintent.Resolution
	found      bool
}

func (*fakeLocalAgentConfigIdempotency) Prepare(
	context.Context,
	string,
	localagent.Config,
) (localAgentConfigEvidence, error) {
	return localAgentConfigEvidence{}, nil
}

func (idempotency *fakeLocalAgentConfigIdempotency) ResolveExisting(
	context.Context,
	etcd.IdempotencyLocator,
	localAgentConfigEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.resolution, idempotency.found, nil
}

func (idempotency *fakeLocalAgentConfigIdempotency) ResolveKnown(
	context.Context,
	localAgentConfigEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

func (idempotency *fakeLocalAgentConfigIdempotency) ResolveUnknown(
	context.Context,
	etcd.IdempotencyLocator,
	localAgentConfigEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.resolution, nil
}

// Rationale: a committed config mutation must replay its exact response before
// any target lookup so deletion or later config changes cannot alter a retry.
func TestLocalAgentConfigReplayDoesNotReadTheAgent(t *testing.T) {
	t.Parallel()

	want := []byte(`{"labels":{"zone":"edge"},"max_concurrent_tasks":2,"pull_interval_seconds":5}`)
	repository := &fakeLocalAgentRecords{}
	idempotency := &fakeLocalAgentConfigIdempotency{
		found: true,
		resolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay,
			Response: etcd.IdempotencyResponse{
				Status: 200, ContentKind: "application/json", Body: want,
			},
		},
	}
	adapter, err := newLocalAgentRepositoryAdapter(repository, idempotency)
	if err != nil {
		t.Fatalf("newLocalAgentRepositoryAdapter() error = %v", err)
	}
	result, err := adapter.UpdateConfig(context.Background(), runtimeAdapterAgentID, localagent.Config{
		PullIntervalSeconds: 5,
		MaxConcurrentTasks:  2,
		Labels:              map[string]string{"zone": "edge"},
	}, "agent-config-key-0001")
	if err != nil {
		t.Fatalf("UpdateConfig() error = %v", err)
	}
	if result.Applied || repository.getCalls != 0 || !bytes.Equal(result.ResponseBody, want) {
		t.Fatalf("UpdateConfig() = %#v, GetSingleton calls = %d", result, repository.getCalls)
	}
}
