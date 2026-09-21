package backup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backupRunTestCipher struct{}

func (backupRunTestCipher) Seal(_ context.Context, plaintext []byte) ([]byte, error) {
	return append([]byte("sealed:"), plaintext...), nil
}

func (backupRunTestCipher) Open(_ context.Context, ciphertext []byte) ([]byte, error) {
	return append([]byte(nil), ciphertext...), nil
}

type backupRunReplayRepository struct{ prepares int }

func (repository *backupRunReplayRepository) PrepareBackupRun(
	context.Context,
	BackupRunPrepareInput,
) (BackupRunPrepared, error) {
	repository.prepares++
	return BackupRunPrepared{}, nil
}

func (*backupRunReplayRepository) PrepareBackupRunRetry(
	context.Context,
	BackupRunRetryPrepareInput,
) (BackupRunRetryPrepared, error) {
	return BackupRunRetryPrepared{}, nil
}

type backupRunReplayPlans struct{}

func (backupRunReplayPlans) BuildBackupRunPlan(
	BackupRunPlanInput,
) (*agentpb.ExecutionPlan, error) {
	return nil, nil
}

type backupRunReplayIdempotency struct{}

func (backupRunReplayIdempotency) Prepare(context.Context, string, string) (backupRunEvidence, error) {
	return backupRunEvidence{}, nil
}

func (backupRunReplayIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

	backupRunEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotentintent.Resolution{
		Kind: idempotentintent.ResolutionReplay,
		Response: testidempotency.IdempotencyResponse{
			Status: 202,
			Body:   []byte(`{"task_id":"task_replayed"}`),
		},
	}, true, nil
}

func (backupRunReplayIdempotency) NewMarker(
	backupRunEvidence, testidempotency.IdempotencyLocator, testidempotency.IdempotencyResponse, string,

	time.Time,
) (testidempotency.IdempotencyMarker, error) {
	panic("replay must not create a marker")
}

func (backupRunReplayIdempotency) ResolveKnown(
	context.Context,
	backupRunEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	panic("replay must not resolve a known publication")
}

func (backupRunReplayIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	backupRunEvidence,
	error,
) (idempotentintent.Resolution, error) {
	panic("replay must not resolve an unknown publication")
}

type backupRunServicePublication struct {
	err       error
	publishes int
}

func (publication *backupRunServicePublication) Publish(
	_ context.Context,
	_ etcd.TaskRecord,
	_ *agentpb.ExecutionPlan,
	_ testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	publication.publishes++
	return etcd.IdempotencyTransactionResult{}, publication.err
}

func (*backupRunServicePublication) Clear() {}

type backupRunServiceRepository struct {
	publication *backupRunServicePublication
	prepares    int
}

func (repository *backupRunServiceRepository) PrepareBackupRun(
	_ context.Context,
	input BackupRunPrepareInput,
) (BackupRunPrepared, error) {
	repository.prepares++
	return BackupRunPrepared{
		Run: testbackupruntime.BackupRunRecord{
			TaskID:        input.TaskID,
			OperationID:   input.OperationID,
			EnvironmentID: input.EnvironmentID,
		},
		Publication: repository.publication,
	}, nil
}

func (*backupRunServiceRepository) PrepareBackupRunRetry(
	context.Context,
	BackupRunRetryPrepareInput,
) (BackupRunRetryPrepared, error) {
	return BackupRunRetryPrepared{}, nil
}

type backupRunServicePlans struct{}

func (backupRunServicePlans) BuildBackupRunPlan(
	BackupRunPlanInput,
) (*agentpb.ExecutionPlan, error) {
	return &agentpb.ExecutionPlan{PlanHash: make([]byte, 32)}, nil
}

type backupRunServiceIdempotency struct {
	existing    idempotentintent.Resolution
	existingSet bool
	existingErr error
	known       idempotentintent.Resolution
	unknown     idempotentintent.Resolution
}

func (backupRunServiceIdempotency) Prepare(context.Context, string, string) (backupRunEvidence, error) {
	return backupRunEvidence{}, nil
}

func (idempotency backupRunServiceIdempotency) ResolveExisting(
	context.Context, testidempotency.IdempotencyLocator,

	backupRunEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.existing, idempotency.existingSet, idempotency.existingErr
}

func (backupRunServiceIdempotency) NewMarker(
	_ backupRunEvidence,
	locator testidempotency.IdempotencyLocator,
	response testidempotency.IdempotencyResponse,
	taskID string,
	now time.Time,
) (testidempotency.IdempotencyMarker, error) {
	return testidempotency.IdempotencyMarker{
		Kind: testidempotency.IdempotencyMarkerTask, State: testidempotency.IdempotencyMarkerPending,
		Locator: locator, Response: response, TaskID: taskID, CreatedAt: now, UpdatedAt: now,
	}, nil
}

func (idempotency backupRunServiceIdempotency) ResolveKnown(
	context.Context,
	backupRunEvidence,
	etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}

func (idempotency backupRunServiceIdempotency) ResolveUnknown(
	context.Context, testidempotency.IdempotencyLocator,

	backupRunEvidence,
	error,
) (idempotentintent.Resolution, error) {
	return idempotency.unknown, nil
}

// Rationale: exact replay must not derive a second durable candidate.
func TestBackupRunServiceReplaysBeforePreparation(t *testing.T) {
	repository := &backupRunReplayRepository{}
	service := NewBackupRunService(repository, backupRunReplayPlans{}, backupRunReplayIdempotency{})
	response, err := service.RunBackup(
		context.Background(),
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"manual-replay",
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != 202 || repository.prepares != 0 {
		t.Fatalf(
			"response=%v prepares=%d, want replay before state preparation",
			response,
			repository.prepares,
		)
	}
}

// Rationale: a newly published Task marker is pending and receives retention
// only when Task terminalization commits its applied lifecycle state.
func TestDurableBackupRunNewMarkerHasNoPendingRetention(t *testing.T) {
	protector, err := secretvalue.NewProtector(backupRunTestCipher{}, backupRunTestCipher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	service := &durableBackupRunIdempotency{coordinator: coordinator}
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	evidence, err := service.Prepare(context.Background(), environmentID, backupRunRoute)
	if err != nil {
		t.Fatal(err)
	}
	defer evidence.candidate.Destroy()
	now := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, now, 1)
	marker, err := service.NewMarker(
		evidence, testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: "POST", Route: backupRunRoute, Key: "manual-backup-key-0001",
		}, testidempotency.IdempotencyResponse{
			Status: 202, ContentKind: "application/json",
			Body: []byte(`{"task_id":"` + taskID + `"}`),
		}, taskID,
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	if !marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() ||
		marker.State != testidempotency.IdempotencyMarkerPending {
		t.Fatalf("pending marker lifecycle = %#v", marker)
	}
}

// Rationale: a fresh internal request returns the exact applied response only
// after one publication attempt has been classified as applied.
func TestBackupRunServicePublishesFreshAppliedRun(t *testing.T) {
	publication := &backupRunServicePublication{}
	repository := &backupRunServiceRepository{publication: publication}
	service := NewBackupRunService(repository, backupRunServicePlans{}, backupRunServiceIdempotency{
		known: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	})
	service.now = func() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) }
	response, err := service.RunBackup(
		context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", "manual-backup-key-0002",
	)
	if err != nil || response.Status != 202 || publication.publishes != 1 ||
		repository.prepares != 1 {
		t.Fatalf(
			"RunBackup() = %#v, %v publishes=%d prepares=%d",
			response,
			err,
			publication.publishes,
			repository.prepares,
		)
	}
}

// Rationale: an unknown commit outcome must resolve the marker rather than
// publish a second candidate or expose storage ambiguity to the operator.
func TestBackupRunServiceResolvesUnknownPublication(t *testing.T) {
	want := testidempotency.IdempotencyResponse{
		Status:      202,
		ContentKind: "application/json",
		Body:        []byte(`{"task_id":"task_replayed"}`),
	}
	publication := &backupRunServicePublication{
		err: errs.New(errs.KindStorageUnavailable, "unknown commit"),
	}
	repository := &backupRunServiceRepository{publication: publication}
	service := NewBackupRunService(repository, backupRunServicePlans{}, backupRunServiceIdempotency{
		unknown: idempotentintent.Resolution{
			Kind:     idempotentintent.ResolutionReplay,
			Response: want,
		},
	})
	response, err := service.RunBackup(
		context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", "manual-backup-key-0003",
	)
	if err != nil || response.Status != want.Status || publication.publishes != 1 ||
		repository.prepares != 1 {
		t.Fatalf(
			"RunBackup(unknown) = %#v, %v publishes=%d prepares=%d",
			response,
			err,
			publication.publishes,
			repository.prepares,
		)
	}
}

// Rationale: marker mismatch and an active winning Task both fail before any
// fixed-revision candidate is derived or published.
func TestBackupRunServiceRejectsExistingMarkerConflictBeforePreparation(t *testing.T) {
	for _, test := range []struct {
		name string
		kind errs.Kind
	}{
		{name: "marker mismatch", kind: errs.KindIdempotencyMismatch},
		{name: "in progress", kind: errs.KindIdempotencyInProgress},
	} {
		t.Run(test.name, func(t *testing.T) {
			publication := &backupRunServicePublication{}
			repository := &backupRunServiceRepository{publication: publication}
			service := NewBackupRunService(
				repository,
				backupRunServicePlans{},
				backupRunServiceIdempotency{
					existingErr: errs.New(test.kind, test.name),
				},
			)
			if _, err := service.RunBackup(
				context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", "manual-backup-key-0004",
			); err == nil || repository.prepares != 0 || publication.publishes != 0 {
				t.Fatalf(
					"RunBackup(%s) error=%v prepares=%d publishes=%d",
					test.name,
					err,
					repository.prepares,
					publication.publishes,
				)
			}
		})
	}
}

type backupRunLocatorCapture struct {
	backupRunServiceIdempotency
	locators []testidempotency.IdempotencyLocator
}

func (capture *backupRunLocatorCapture) ResolveExisting(
	_ context.Context,
	locator testidempotency.IdempotencyLocator,
	_ backupRunEvidence,
) (idempotentintent.Resolution, bool, error) {
	capture.locators = append(capture.locators, locator)
	return idempotentintent.Resolution{}, false, nil
}

// Rationale: an operator may deliberately submit the system key bytes, but
// public and scheduled attempts still resolve and replay under disjoint routes.
func TestBackupRunScheduledIdempotencyNamespaceCannotCollideWithOperatorHeader(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 27, 11, 0, 0, 0, time.UTC)
	evaluatedAt := scheduledAt.Add(time.Hour)
	key := fmt.Sprintf("scheduled:%d:%s", 42, scheduledAt.Format("20060102T150405Z"))
	publication := &backupRunServicePublication{}
	repository := &backupRunServiceRepository{publication: publication}
	capture := &backupRunLocatorCapture{}
	service := NewBackupRunService(repository, backupRunServicePlans{}, capture)
	service.now = func() time.Time { return evaluatedAt }

	if _, err := service.RunBackup(
		context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", key,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := service.RunScheduledBackup(
		context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		42, scheduledAt, evaluatedAt,
	); err != nil {
		t.Fatal(err)
	}
	if len(capture.locators) != 2 ||
		capture.locators[0].Key != capture.locators[1].Key ||
		capture.locators[0].Route != backupRunRoute ||
		capture.locators[1].Route != scheduledBackupRunRoute ||
		capture.locators[0].Route == capture.locators[1].Route {
		t.Fatalf("idempotency locators = %#v", capture.locators)
	}
}
