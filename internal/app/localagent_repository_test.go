package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: encrypted credential material and mutable labels must cross the
// app boundary without shared backing storage while every CAS identity remains
// exact through ready, delete-begin, and final-delete transitions.
func TestLocalAgentRepositoryAdapterTranslatesLifecycleWithoutAliasing(t *testing.T) {
	t.Parallel()

	repository := &fakeLocalAgentRecords{}
	adapter, err := newLocalAgentRepositoryAdapter(repository)
	if err != nil {
		t.Fatalf("newLocalAgentRepositoryAdapter() error = %v", err)
	}
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	record := localagent.Record{
		ID: runtimeAdapterAgentID, EnrollmentTaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Image: testAppAgentImage, Generation: 7,
		Phase: localagent.PhaseProvisioning,
		Config: localagent.Config{
			PullIntervalSeconds: 5, MaxConcurrentTasks: 4,
			Labels: map[string]string{"role": "local"},
		},
		Credential: localagent.Credential{EncryptedToken: []byte("encrypted"), Digest: "digest"},
		CreatedAt:  now,
	}
	created, err := adapter.CreateSingleton(context.Background(), record)
	if err != nil {
		t.Fatalf("CreateSingleton() error = %v", err)
	}
	record.Config.Labels["role"] = "mutated"
	record.Credential.EncryptedToken[0] = 'X'
	if repository.created.Config.Labels["role"] != "local" ||
		string(repository.created.EncryptedToken) != "encrypted" ||
		!repository.created.TokenUpdatedAt.Equal(now) {
		t.Fatalf("durable create = %#v", repository.created)
	}
	repository.created.Config.Labels["role"] = "infrastructure-mutated"
	repository.created.EncryptedToken[0] = 'Y'
	if created.Record.Config.Labels["role"] != "local" ||
		string(created.Record.Credential.EncryptedToken) != "encrypted" || created.Revision != 11 {
		t.Fatalf("created lifecycle record = %#v", created)
	}

	readyAt := now.Add(time.Second)
	ready, err := adapter.MarkReady(context.Background(), runtimeAdapterAgentID, 7, 11, readyAt)
	if err != nil {
		t.Fatalf("MarkReady() error = %v", err)
	}
	if repository.markGeneration != 7 || repository.markRevision != 11 ||
		!repository.markReadyAt.Equal(readyAt) || !ready.Record.ReadyAt.Equal(readyAt) ||
		ready.Record.Phase != localagent.PhaseReady || ready.Revision != 12 {
		t.Fatalf("ready transition = %#v, repository = %#v", ready, repository)
	}
	deleting, err := adapter.BeginDelete(context.Background(), runtimeAdapterAgentID, 7, 12)
	if err != nil {
		t.Fatalf("BeginDelete() error = %v", err)
	}
	if deleting.Record.Phase != localagent.PhaseDeleting || deleting.Record.Credential.Digest != "" ||
		deleting.Revision != 13 {
		t.Fatalf("deleting transition = %#v", deleting)
	}
	if err := adapter.Delete(context.Background(), runtimeAdapterAgentID, 7, 13); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if repository.deleteGeneration != 7 || repository.deleteRevision != 13 {
		t.Fatalf("delete CAS = generation %d revision %d", repository.deleteGeneration, repository.deleteRevision)
	}
}

// Rationale: future or corrupt durable phases must not silently acquire a
// lifecycle meaning at the application boundary.
func TestLocalAgentRepositoryAdapterRejectsUnknownDurablePhase(t *testing.T) {
	t.Parallel()

	repository := &fakeLocalAgentRecords{get: etcd.Versioned[etcd.LocalAgentRecord]{
		Record: etcd.LocalAgentRecord{Phase: "future"}, Revision: 1,
	}}
	adapter, err := newLocalAgentRepositoryAdapter(repository)
	if err != nil {
		t.Fatalf("newLocalAgentRepositoryAdapter() error = %v", err)
	}
	if _, err := adapter.GetSingleton(context.Background()); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("GetSingleton() error = %v, want internal", err)
	}
}

type fakeLocalAgentRecords struct {
	created          etcd.LocalAgentRecord
	get              etcd.Versioned[etcd.LocalAgentRecord]
	markGeneration   uint64
	markRevision     int64
	markReadyAt      time.Time
	deleteGeneration uint64
	deleteRevision   int64
}

func (repository *fakeLocalAgentRecords) CreateSingleton(
	_ context.Context,
	record etcd.LocalAgentRecord,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	repository.created = record
	return etcd.Versioned[etcd.LocalAgentRecord]{Record: record, Revision: 11}, nil
}

func (repository *fakeLocalAgentRecords) GetSingleton(
	_ context.Context,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	return repository.get, nil
}

func (repository *fakeLocalAgentRecords) UpdateConfig(
	_ context.Context,
	_ string,
	_ uint64,
	_ int64,
	config etcd.LocalAgentConfig,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	record := repository.created
	record.Config = config
	return etcd.Versioned[etcd.LocalAgentRecord]{Record: record, Revision: 11}, nil
}

func (repository *fakeLocalAgentRecords) MarkReady(
	_ context.Context,
	_ string,
	generation uint64,
	revision int64,
	readyAt time.Time,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	repository.markGeneration = generation
	repository.markRevision = revision
	repository.markReadyAt = readyAt
	record := repository.created
	record.Phase = etcd.LocalAgentPhaseReady
	record.ReadyAt = readyAt
	return etcd.Versioned[etcd.LocalAgentRecord]{Record: record, Revision: 12}, nil
}

func (repository *fakeLocalAgentRecords) BeginDelete(
	_ context.Context,
	_ string,
	_ uint64,
	_ int64,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	record := repository.created
	record.Phase = etcd.LocalAgentPhaseDeleting
	record.TokenDigest = ""
	return etcd.Versioned[etcd.LocalAgentRecord]{Record: record, Revision: 13}, nil
}

func (repository *fakeLocalAgentRecords) Delete(
	_ context.Context,
	_ string,
	generation uint64,
	revision int64,
) error {
	repository.deleteGeneration = generation
	repository.deleteRevision = revision
	return nil
}
