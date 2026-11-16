package secrets

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secretremoval"
	testsecrets "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: acceptance preserves only metadata and replay identity; native value
// revision capture and deletion authority belong to preparation of that same Task.
func TestSecretDeletionAcceptsPureIntentThenCapturesOriginalTask(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 22, 23, 0, 0, 0, time.UTC)
	projectID := ids.NewAt(ids.KindProject, now, 1)
	tenantID := ids.NewAt(ids.KindTenant, now, 3)
	secretID := ids.NewAt(ids.KindSecret, now, 2)
	record, err := testsecrets.NewProjectRecord(
		secretID, projectID, "TOKEN", core.SecretKindEnvVar, "", now,
	)
	if err != nil {
		t.Fatalf("NewProjectSecretRecord() error = %v", err)
	}
	repository := &fakeSecretDeletionRepository{
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID: projectID, TenantID: tenantID, Kind: testhierarchy.ProjectKindTenant,
			},
			Revision: 5, ReadRevision: 5,
		},
		secret: testkeyvalue.Versioned[testsecrets.Record]{Record: record, Revision: 7, ReadRevision: 7},
	}
	idempotency := &fakeSecretDeletionIdempotency{}
	acceptance := &fakeSecretDeletionAcceptance{}
	service := &secretDeletionService{
		repository:  repository,
		idempotency: idempotency,
		acceptance:  acceptance,
		now:         time.Now,
	}
	service.now = func() time.Time { return now }
	response, err := service.DeleteSecret(
		context.Background(),
		secretID,
		apiTypes.TaskRunAfterRequest{After: []string{ids.NewAt(ids.KindTask, now, 7)}},
		"secret-delete-key-0001",
	)
	if err != nil {
		t.Fatalf("DeleteSecret() error = %v", err)
	}
	var accepted apiTypes.TaskAccepted
	if err := json.Unmarshal(response.Body, &accepted); err != nil {
		t.Fatalf("decode TaskAccepted: %v", err)
	}
	if repository.captureCalls != 0 || repository.beginCalls != 0 || acceptance.calls != 1 ||
		acceptance.task.ExpectedItems != 0 || acceptance.task.Intent != (workexecution.Input{}) || len(acceptance.task.After) != 1 || !reflect.DeepEqual(acceptance.task.After, acceptance.request.After) ||
		!acceptance.task.PendingPreparation() || accepted.TaskID != acceptance.task.ID ||
		acceptance.submission.ReplayTarget == nil || *acceptance.submission.ReplayTarget != (testidempotency.IdempotencyReplayTarget{Kind: testidempotency.IdempotencyReplayTargetSecret, ID: secretID}) ||
		acceptance.submission.Locator.ScopeKind != testidempotency.IdempotencyScopeProject || acceptance.submission.Locator.ScopeID != projectID {
		t.Fatalf("acceptance captured native state or lost replay metadata: %#v / %#v", acceptance, repository)
	}
	prepared, err := service.prepareAcceptedDeletion(
		context.Background(),
		workqueue.TaskSnapshot{Task: acceptance.task, Revision: 1},
		0,
		now.Add(5*time.Second),
		acceptance.request,
	)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := repository.input.Reference()
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != http.StatusAccepted || prepared.Task.ID != accepted.TaskID ||
		prepared.Task.Intent != reference || prepared.Task.ExpectedItems != 1 ||
		prepared.Task.Scope.TenantID != tenantID || prepared.Task.Scope.ProjectID != projectID ||
		!prepared.Task.Deadline.Equal(now.Add(24*time.Hour)) ||
		repository.input.TaskID != accepted.TaskID || repository.captureCalls != 1 || repository.beginCalls != 1 ||
		repository.input.SecretRevision != repository.secret.Revision || len(prepared.Graph) != 1 ||
		prepared.Graph[0].TaskID != accepted.TaskID || !prepared.Graph[0].Deadline.Equal(now.Add(35*time.Second)) || workexecution.ValidateGraph(prepared.Task, prepared.Graph) != nil {
		t.Fatalf("late preparation did not preserve original Task/deadline/authority: %#v / %#v", prepared, repository)
	}

}

// Rationale: exact retries must resolve through the stable Secret replay index
// before reading a target that successful finalization has already removed.
func TestSecretDeletionReplaysBeforeTargetLookup(t *testing.T) {
	t.Parallel()
	secretID := "sec_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	want := testidempotency.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	repository := &fakeSecretDeletionRepository{}
	idempotency := &fakeSecretDeletionIdempotency{
		indexed: true,
		locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopePlatform, ScopeID: "-",
			Method: http.MethodDelete, Route: secretDeletionRoute, Key: "secret-delete-key-0002",
		},
		existing: true,
		existingResolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay, Response: want,
		},
	}
	acceptance := &fakeSecretDeletionAcceptance{}
	service := &secretDeletionService{
		repository:  repository,
		idempotency: idempotency,
		acceptance:  acceptance,
		now:         time.Now,
	}
	got, err := service.DeleteSecret(
		context.Background(),
		secretID,
		apiTypes.TaskRunAfterRequest{},
		"secret-delete-key-0002",
	)
	if err != nil || !reflect.DeepEqual(got, want) || repository.readCalls != 0 || repository.beginCalls != 0 {
		t.Fatalf(
			"DeleteSecret(replay) = %#v/%v, repository calls = %d/%d",
			got,
			err,
			repository.readCalls,
			repository.beginCalls,
		)
	}
}

type fakeSecretDeletionRepository struct {
	project      testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	secret       testkeyvalue.Versioned[testsecrets.Record]
	input        secretremoval.Input
	task         workexecution.Task
	readCalls    int
	captureCalls int
	beginCalls   int
}

func (repository *fakeSecretDeletionRepository) GetProject(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	repository.readCalls++
	return repository.project, nil
}

func (repository *fakeSecretDeletionRepository) GetSecret(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testsecrets.Record], error) {
	repository.readCalls++
	return repository.secret, nil
}

func (repository *fakeSecretDeletionRepository) CaptureSecretRemoval(
	_ context.Context,
	owner testsecrets.Owner,
	current testkeyvalue.Versioned[testsecrets.Record],
	taskID, workID, planID string, at time.Time,
) (secretremoval.Input, error) {
	input := secretremoval.Input{TaskID: taskID, WorkItemID: workID, PlanID: planID,
		SecretID: current.Record.Secret.ID, SecretRevision: current.Revision,
		ValueRevision: 8, CreatedAt: at}
	if owner.Project != nil {
		input.ProjectID, input.TenantID = owner.Project.Record.ID, owner.Project.Record.TenantID
	}
	repository.captureCalls++
	repository.input = input
	return input, nil
}

func (repository *fakeSecretDeletionRepository) PrepareSecretRemovalAuthority(
	_ context.Context,
	_ testsecrets.Owner,
	_ testkeyvalue.Versioned[testsecrets.Record],
	input secretremoval.Input,
	task workexecution.Task,
) (workqueue.Change, error) {
	repository.beginCalls++
	repository.input, repository.task = input, task
	return workqueue.Change{}, nil
}

type fakeSecretDeletionIdempotency struct {
	locator            testidempotency.IdempotencyLocator
	indexed            bool
	existing           bool
	existingResolution idempotentintent.Resolution
}

func (idempotency *fakeSecretDeletionIdempotency) ResolveReplayLocator(
	_ context.Context,
	_ testidempotency.IdempotencyReplayTarget,
	_ string,
	_ string,
	_ string,
) (testidempotency.IdempotencyLocator, bool, error) {
	return idempotency.locator, idempotency.indexed, nil
}

func (idempotency *fakeSecretDeletionIdempotency) Prepare(
	_ context.Context,
	_ testidempotency.IdempotencyLocator,
	_ string,
	_ []string,
) (secretDeletionEvidence, error) {
	return secretDeletionEvidence{}, nil
}

func (idempotency *fakeSecretDeletionIdempotency) ResolveExisting(
	_ context.Context,
	_ testidempotency.IdempotencyLocator,
	_ secretDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.existingResolution, idempotency.existing, nil
}

type fakeSecretDeletionAcceptance struct {
	calls      int
	submission acceptedaction.Submission
	request    secretDeletionRequest
	task       workexecution.Task
}

func (acceptance *fakeSecretDeletionAcceptance) SubmitSecretDeletion(
	_ context.Context,
	submission acceptedaction.Submission,
	request secretDeletionRequest,
) (testidempotency.IdempotencyResponse, error) {
	acceptance.calls++
	acceptance.submission, acceptance.request = submission, request
	acceptance.task = submission.Task
	// The protected shared acceptance boundary supplies this sealed request handle.
	acceptance.task.AcceptedIntent = &workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: acceptance.task.ID})
	return testidempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}, err
}
