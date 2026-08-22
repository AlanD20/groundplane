package localagent

import (
	"context"
	"encoding/base64"
	"testing"
	"time"
)

func TestUpdateConfigCommitsBeforeRuntimeMaterializationAndCopiesState(t *testing.T) {
	t.Parallel()

	record := configTestRecord()
	repository := &configTestRepository{
		stored:       StoredRecord{Record: record, Revision: 11},
		responseBody: []byte(`{"pull_interval_seconds":5,"max_concurrent_tasks":2,"labels":{"zone":"edge"}}`),
	}
	runtime := &configTestRuntime{repository: repository}
	manager, err := New(Dependencies{
		Repository: repository,
		Runtime:    runtime,
		Container:  configTestContainer{},
		Sessions:   configTestSessions{},
		Tasks:      configTestTasks{},
		Clock:      SystemClock{},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	desired := Config{PullIntervalSeconds: 5, MaxConcurrentTasks: 2, Labels: map[string]string{"zone": "edge"}}
	response, err := manager.UpdateConfig(context.Background(), record.ID, desired, "agent-config-key-0001")
	if err != nil {
		t.Fatalf("UpdateConfig() error = %v", err)
	}
	if !repository.updated || !runtime.materialized || runtime.beforeCommit {
		t.Fatalf(
			"update ordering = repository %t, runtime %t, before commit %t",
			repository.updated,
			runtime.materialized,
			runtime.beforeCommit,
		)
	}
	desired.Labels["zone"] = "also-changed"
	if repository.stored.Record.Config.Labels["zone"] != "edge" || runtime.material.Config.Labels["zone"] != "edge" {
		t.Fatal("UpdateConfig() leaked a mutable labels map")
	}
	if repository.key != "agent-config-key-0001" || string(response) != string(repository.responseBody) {
		t.Fatalf("UpdateConfig() key/response = %q/%q", repository.key, response)
	}
}

func configTestRecord() Record {
	createdAt := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	return Record{
		ID:               "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		EnrollmentTaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Image:            "example.com/groundplane-agent@sha256:0000000000000000000000000000000000000000000000000000000000000000",
		Generation:       1,
		Phase:            PhaseReady,
		Config:           Config{PullIntervalSeconds: 2, MaxConcurrentTasks: 3, Labels: map[string]string{}},
		Credential: Credential{
			EncryptedToken: []byte("ciphertext"),
			Digest:         base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		},
		CreatedAt: createdAt,
		ReadyAt:   createdAt,
	}
}

type configTestRepository struct {
	stored       StoredRecord
	updated      bool
	key          string
	responseBody []byte
}

func (repository *configTestRepository) CreateSingleton(context.Context, Record) (StoredRecord, error) {
	return StoredRecord{}, nil
}
func (repository *configTestRepository) GetSingleton(context.Context) (StoredRecord, error) {
	return StoredRecord{Record: cloneRecord(repository.stored.Record), Revision: repository.stored.Revision}, nil
}
func (repository *configTestRepository) UpdateConfig(
	_ context.Context,
	_ string,
	config Config,
	key string,
) (ConfigUpdateResult, error) {
	repository.updated = true
	repository.key = key
	repository.stored.Record.Config = cloneConfig(config)
	return ConfigUpdateResult{
		Applied:      true,
		Stored:       StoredRecord{Record: cloneRecord(repository.stored.Record), Revision: repository.stored.Revision},
		ResponseBody: append([]byte(nil), repository.responseBody...),
	}, nil
}

func (repository *configTestRepository) MarkReady(
	context.Context,
	string,
	uint64,
	int64,
	time.Time,
) (StoredRecord, error) {
	return StoredRecord{}, nil
}
func (repository *configTestRepository) BeginDelete(context.Context, string, uint64, int64) (StoredRecord, error) {
	return StoredRecord{}, nil
}
func (repository *configTestRepository) Delete(context.Context, string, uint64, int64) error {
	return nil
}

type configTestRuntime struct {
	repository   *configTestRepository
	materialized bool
	beforeCommit bool
	material     RuntimeMaterial
}

func (runtime *configTestRuntime) GenerateCredential(context.Context, string) (Credential, error) {
	return Credential{}, nil
}
func (runtime *configTestRuntime) Materialize(_ context.Context, material RuntimeMaterial) error {
	runtime.materialized = true
	runtime.beforeCommit = !runtime.repository.updated
	runtime.material = material
	return nil
}
func (runtime *configTestRuntime) Remove(context.Context, string) error { return nil }

type configTestContainer struct{}

func (configTestContainer) Converge(context.Context, ContainerDesired) error { return nil }
func (configTestContainer) Remove(context.Context, string, uint64) error     { return nil }

type configTestSessions struct{}

func (configTestSessions) Ready(context.Context, string, uint64) (<-chan struct{}, error) {
	return make(chan struct{}), nil
}
func (configTestSessions) Snapshot(string) (SessionSnapshot, bool)               { return SessionSnapshot{}, false }
func (configTestSessions) StopAssignments(context.Context, string, uint64) error { return nil }
func (configTestSessions) Revoke(context.Context, string, uint64) error          { return nil }
func (configTestSessions) WaitOffline(context.Context, string, uint64) error     { return nil }

type configTestTasks struct{}

func (configTestTasks) AbortActive(context.Context, string, uint64, int32, string) error { return nil }
