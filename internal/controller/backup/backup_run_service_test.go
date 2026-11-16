package backup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
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
	task      workexecution.Task
	item      workexecution.Item
	input     *agentpb.BackupWorkInput
}

func (publication *backupRunServicePublication) PublishWork(
	_ context.Context,
	task workexecution.Task,
	item workexecution.Item,
	input *agentpb.BackupWorkInput,
	_ testidempotency.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	publication.publishes++
	publication.task, publication.item, publication.input = task, item, proto.CloneOf(input)
	return etcd.IdempotencyTransactionResult{}, publication.err
}

func (*backupRunServicePublication) Clear() {}

type backupRunServiceRepository struct {
	t           *testing.T
	publication *backupRunServicePublication
	prepares    int
}

func (repository *backupRunServiceRepository) PrepareBackupRun(
	_ context.Context,
	input BackupRunPrepareInput,
) (BackupRunPrepared, error) {
	repository.prepares++
	prepared := backupRunWorkFixture(repository.t, input)
	prepared.Publication = repository.publication
	prepared.Run.Initiator = input.Initiator
	return prepared, nil
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

// TASK-101: exact replay must not derive a second durable candidate.
func TestBackupRunServiceReplaysBeforePreparation(t *testing.T) {
	repository := &backupRunReplayRepository{}
	service := newBackupRunFixtureService(t, repository, backupRunReplayIdempotency{})
	response, err := service.RunBackup(
		context.Background(),
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"manual-replay", api.TaskRunAfterRequest{},
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

// TASK-101: a fresh internal request returns the exact applied response only
// after one publication attempt has been classified as applied.
func TestBackupRunServiceCapturesOnlyWhenAcceptedCompilerRuns(t *testing.T) {
	publication := &backupRunServicePublication{}
	repository := &backupRunServiceRepository{t: t, publication: publication}
	service := newBackupRunFixtureService(t, repository, backupRunServiceIdempotency{})
	at := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	task := workexecution.Task{
		ID:        ids.New(ids.KindTask),
		Actor:     workexecution.OperatorActor,
		Action:    workexecution.ActionBackup,
		TargetID:  "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(acceptedaction.QueueTimeout),
	}
	fixture := backupRunWorkFixture(
		t,
		BackupRunPrepareInput{
			EnvironmentID: task.TargetID,
			TaskID:        task.ID,
			OperationID:   ids.New(ids.KindOperation),
			PlanID:        ids.New(ids.KindPlan),
			CreatedAt:     at,
		},
	)
	task.Scope = workexecution.Scope{
		TenantID:      fixture.Owner.TenantID,
		ProjectID:     fixture.Owner.ProjectID,
		EnvironmentID: task.TargetID,
	}
	prepared, err := service.prepareAcceptedRun(
		t.Context(),
		workqueue.TaskSnapshot{Task: task, Revision: 1},
		1,
		at,
		acceptedBackupRun{},
	)
	if err != nil || repository.prepares != 1 || publication.publishes != 1 || prepared.Task.ID != task.ID ||
		len(prepared.Graph) != 1 {
		t.Fatalf("late capture: %#v / %v", prepared, err)
	}
	if prepared.Graph[0].Input == nil || *prepared.Graph[0].Input != prepared.Task.Intent {
		t.Fatal("compiled Backup lost captured input")
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
	repository := &backupRunServiceRepository{t: t, publication: publication}
	service := newBackupRunFixtureService(t, repository, backupRunServiceIdempotency{
		unknown: idempotentintent.Resolution{
			Kind:     idempotentintent.ResolutionReplay,
			Response: want,
		},
	})
	response, err := service.RunScheduledBackup(
		context.Background(),
		"env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		42,
		time.Now().UTC(),
		time.Now().UTC(),
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
			repository := &backupRunServiceRepository{t: t, publication: publication}
			service := newBackupRunFixtureService(t,
				repository,
				backupRunServiceIdempotency{
					existingErr: errs.New(test.kind, test.name),
				},
			)
			if _, err := service.RunBackup(
				context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", "manual-backup-key-0004", api.TaskRunAfterRequest{},
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
	if locator.Route == backupRunRoute {
		return idempotentintent.Resolution{
			Kind:     idempotentintent.ResolutionReplay,
			Response: testidempotency.IdempotencyResponse{Status: 202, Body: []byte(`{"task_id":"replay"}`)},
		}, true, nil
	}
	return idempotentintent.Resolution{}, false, nil
}

// Rationale: an operator may deliberately submit the system key bytes, but
// public and scheduled attempts still resolve and replay under disjoint routes.
func TestBackupRunScheduledIdempotencyNamespaceCannotCollideWithOperatorHeader(t *testing.T) {
	scheduledAt := time.Date(2026, 8, 27, 11, 0, 0, 0, time.UTC)
	evaluatedAt := scheduledAt.Add(time.Hour)
	key := fmt.Sprintf("scheduled:%d:%s", 42, scheduledAt.Format("20060102T150405Z"))
	publication := &backupRunServicePublication{}
	repository := &backupRunServiceRepository{t: t, publication: publication}
	capture := &backupRunLocatorCapture{}
	service := newBackupRunFixtureService(t, repository, capture)
	service.now = func() time.Time { return evaluatedAt }

	if _, err := service.RunBackup(
		context.Background(), "env_01ARZ3NDEKTSV4RRFFQ69G5FAV", key, api.TaskRunAfterRequest{},
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

func newBackupRunFixtureService(
	t *testing.T,
	repository BackupRunRepository,
	idem backupRunIdempotency,
) *BackupRunService {
	t.Helper()
	protector, err := secretvalue.NewProtector(backupRunTestCipher{}, backupRunTestCipher{})
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := idempotentintent.NewCoordinator(protector)
	if err != nil {
		t.Fatal(err)
	}
	return NewBackupRunService(repository, idem, &acceptedaction.Submitter{}, coordinator)
}
func (*backupRunReplayRepository) ReadBackupTaskAcceptance(context.Context, string) (etcd.BackupTaskAcceptance, error) {
	panic("replay must not read acceptance metadata")
}

func (*backupRunServiceRepository) ReadBackupTaskAcceptance(
	context.Context,
	string,
) (etcd.BackupTaskAcceptance, error) {
	return etcd.BackupTaskAcceptance{}, nil
}

func (publication *backupRunServicePublication) PrepareWorkAuthority(
	ctx context.Context,
	task workexecution.Task,
	item workexecution.Item,
	input *agentpb.BackupWorkInput,
) (workqueue.Change, error) {
	publication.publishes++
	publication.task, publication.item, publication.input = task, item, proto.CloneOf(input)
	return workqueue.Change{}, publication.err
}
