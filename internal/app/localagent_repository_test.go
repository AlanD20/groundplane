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
	adapter, err := newLocalAgentRepositoryAdapter(repository, &fakeLocalAgentConfigIdempotency{})
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
	adapter, err := newLocalAgentRepositoryAdapter(repository, &fakeLocalAgentConfigIdempotency{})
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

func TestLocalAgentRepositoryAdapterTranslatesReplacementWithoutAliasing(t *testing.T) {
	// Rationale: replacement credentials cross the app/etcd seam once and must
	// not share secret-bearing buffers with either side.
	t.Parallel()

	repository := &fakeLocalAgentRecords{}
	adapter, err := newLocalAgentRepositoryAdapter(repository, &fakeLocalAgentConfigIdempotency{})
	if err != nil {
		t.Fatalf("newLocalAgentRepositoryAdapter() error = %v", err)
	}
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	current := localagent.StoredRecord{Record: localagent.Record{
		ID: runtimeAdapterAgentID, EnrollmentTaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Image: testAppAgentImage, Generation: 7, Phase: localagent.PhaseReady,
		Config: localagent.Config{
			PullIntervalSeconds: 5, MaxConcurrentTasks: 4,
			Labels: map[string]string{"role": "local"},
		},
		Credential: localagent.Credential{EncryptedToken: []byte("old-encrypted"), Digest: "old-digest"},
		CreatedAt:  now, ReadyAt: now,
	}, Revision: 11}
	credential := localagent.Credential{EncryptedToken: []byte("new-encrypted"), Digest: "new-digest"}
	replacement, err := adapter.BeginReplacement(
		context.Background(),
		current,
		"ghcr.io/aland20/groundplane-agent@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		credential,
		now.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("BeginReplacement() error = %v", err)
	}
	credential.EncryptedToken[0] = 'X'
	if string(repository.replacement.EncryptedToken) != "new-encrypted" ||
		string(replacement.Record.Credential.EncryptedToken) != "new-encrypted" ||
		replacement.Record.Generation != 8 || replacement.Record.Phase != localagent.PhaseUpdating {
		t.Fatalf("replacement = %#v, durable = %#v", replacement, repository.replacement)
	}
	repository.replacement.EncryptedToken[0] = 'Y'
	if string(replacement.Record.Credential.EncryptedToken) != "new-encrypted" {
		t.Fatal("replacement result aliases durable encrypted token")
	}
	ready, err := adapter.MarkReplacementReady(context.Background(), runtimeAdapterAgentID, 8, 12)
	if err != nil {
		t.Fatalf("MarkReplacementReady() error = %v", err)
	}
	if ready.Record.Phase != localagent.PhaseReady || !ready.Record.ReadyAt.Equal(now) {
		t.Fatalf("replacement Ready = %#v", ready)
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
	getCalls         int
	replacement      etcd.LocalAgentRecord
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
	repository.getCalls++
	return repository.get, nil
}

func (repository *fakeLocalAgentRecords) UpdateConfigIdempotent(
	_ context.Context,
	current etcd.Versioned[etcd.LocalAgentRecord],
	config etcd.LocalAgentConfig,
	_ etcd.IdempotencyMarker,
) (etcd.Versioned[etcd.LocalAgentRecord], etcd.IdempotencyTransactionResult, error) {
	current.Record.Config = config
	return current, etcd.IdempotencyTransactionResult{}, nil
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

func (repository *fakeLocalAgentRecords) ReplaceGeneration(
	_ context.Context,
	current etcd.Versioned[etcd.LocalAgentRecord],
	image string,
	encryptedToken []byte,
	tokenDigest string,
	updatedAt time.Time,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	record := current.Record
	record.Image = image
	record.Generation++
	record.Phase = etcd.LocalAgentPhaseUpdating
	record.EncryptedToken = append([]byte(nil), encryptedToken...)
	record.TokenDigest = tokenDigest
	record.TokenUpdatedAt = updatedAt
	repository.replacement = record
	return etcd.Versioned[etcd.LocalAgentRecord]{Record: record, Revision: 12}, nil
}

func (repository *fakeLocalAgentRecords) MarkReplacementReady(
	_ context.Context,
	_ string,
	_ uint64,
	_ int64,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	record := repository.replacement
	record.Phase = etcd.LocalAgentPhaseReady
	return etcd.Versioned[etcd.LocalAgentRecord]{Record: record, Revision: 13}, nil
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
