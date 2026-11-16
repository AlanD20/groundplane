package connectors

import (
	"context"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	idempotentintent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorremoval"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: first dispatch must derive immutable Environment ownership from
// durable Connector ancestry and publish one exact protected native input.
func TestConnectorDeletionPublishesCapturedNativeRemoval(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	connectorID := ids.NewAt(ids.KindConnector, now, 4)
	repository := &fakeConnectorDeletionRepository{
		project: testkeyvalue.Versioned[testhierarchy.ProjectRecord]{
			Record: testhierarchy.ProjectRecord{
				ID:       projectID,
				TenantID: tenantID,
				Kind:     testhierarchy.ProjectKindTenant,
			},
			Revision: 5, ReadRevision: 5,
		},
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record:   testhierarchy.EnvironmentRecord{ID: environmentID, ProjectID: projectID},
			Revision: 6, ReadRevision: 6,
		},
		connector: testkeyvalue.Versioned[testconnectors.Record]{
			Record: testconnectors.Record{Connector: core.Connector{
				ID: connectorID, EnvironmentID: environmentID, Name: "primary-backups",
			}},
			Revision: 7, ReadRevision: 7,
		},
	}
	idempotency := &fakeConnectorDeletionIdempotency{
		known: idempotentintent.Resolution{Kind: idempotentintent.ResolutionApplied},
	}
	service, err := NewDeletionService(repository, idempotency, &acceptedaction.Submitter{})
	if err != nil {
		t.Fatalf("NewDeletionService() error = %v", err)
	}
	service.now = func() time.Time { return now }
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionRemove,
		TargetID:   connectorID,
		TargetName: "primary-backups",
		Scope:      workexecution.Scope{TenantID: tenantID, ProjectID: projectID, EnvironmentID: environmentID},
		CreatedAt:  now,
		UpdatedAt:  now,
		Deadline:   now.Add(acceptedaction.QueueTimeout),
	}
	if repository.beginCalls != 0 {
		t.Fatal("removal captured before deferred compiler")
	}
	prepared, err := service.prepareAcceptedRemoval(
		t.Context(),
		workqueue.TaskSnapshot{Task: task, Revision: 1},
		1,
		now,
		acceptedConnectorRemoval{},
	)
	if err != nil {
		t.Fatal(err)
	}
	reference, err := repository.input.Reference()
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Task.ID != task.ID || prepared.Task.Scope != task.Scope || prepared.Task.Intent != reference ||
		len(prepared.Graph) != 1 ||
		repository.input.ConnectorRevision != repository.connector.Revision ||
		repository.beginCalls != 1 {
		t.Fatalf("captured removal = %#v / %#v", prepared, repository.input)
	}

}

// Rationale: an exact retry after successful finalization must resolve through
// the stable target index before the deleted Connector is read.
func TestConnectorDeletionReplaysBeforeTargetLookup(t *testing.T) {
	t.Parallel()
	connectorID := "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	want := testidempotency.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"task_01ARZ3NDEKTSV4RRFFQ69G5FAV"}`),
	}
	repository := &fakeConnectorDeletionRepository{}
	idempotency := &fakeConnectorDeletionIdempotency{
		indexed: true,
		locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment,
			ScopeID:   "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			Method:    http.MethodDelete,
			Route:     connectorDeletionRoute,
			Key:       "connector-delete-key-0002",
		},
		existing: true,
		existingResolution: idempotentintent.Resolution{
			Kind: idempotentintent.ResolutionReplay, Response: want,
		},
	}
	service, err := NewDeletionService(repository, idempotency, &acceptedaction.Submitter{})
	if err != nil {
		t.Fatalf("NewDeletionService() error = %v", err)
	}
	got, err := service.DeleteConnector(
		context.Background(),
		connectorID,
		apiTypes.TaskRunAfterRequest{},
		"connector-delete-key-0002",
	)
	if err != nil || got.Status != want.Status || got.ContentKind != want.ContentKind ||
		string(got.Body) != string(want.Body) || repository.readCalls != 0 || repository.beginCalls != 0 {
		t.Fatalf(
			"DeleteConnector(replay) = %#v/%v, repository calls = %d/%d",
			got, err, repository.readCalls, repository.beginCalls,
		)
	}
}

type fakeConnectorDeletionRepository struct {
	project     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	connector   testkeyvalue.Versioned[testconnectors.Record]
	input       connectorremoval.Input
	task        workexecution.Task
	marker      testidempotency.IdempotencyMarker
	readCalls   int
	beginCalls  int
}

func (repository *fakeConnectorDeletionRepository) GetConnector(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testconnectors.Record], error) {
	repository.readCalls++
	return repository.connector, nil
}

func (repository *fakeConnectorDeletionRepository) GetEnvironment(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	repository.readCalls++
	return repository.environment, nil
}

func (repository *fakeConnectorDeletionRepository) GetProject(
	_ context.Context,
	_ string,
) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	repository.readCalls++
	return repository.project, nil
}

func (repository *fakeConnectorDeletionRepository) CaptureConnectorRemoval(
	_ context.Context,
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	current testkeyvalue.Versioned[testconnectors.Record],
	taskID, workID, planID string, at time.Time,
) (connectorremoval.Input, error) {
	return connectorremoval.Input{
		TaskID: taskID, WorkItemID: workID, PlanID: planID,
		TenantID: project.Record.TenantID, ProjectID: project.Record.ID,
		EnvironmentID: environment.Record.ID, ConnectorID: current.Record.Connector.ID,
		ConnectorRevision: current.Revision, CredentialRevision: 8, CreatedAt: at,
	}, nil
}

func (repository *fakeConnectorDeletionRepository) PrepareConnectorRemovalAuthority(
	_ context.Context,
	_ testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	_ testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	_ testkeyvalue.Versioned[testconnectors.Record],
	input connectorremoval.Input,
	task workexecution.Task,
) (workqueue.Change, workexecution.Item, error) {
	repository.beginCalls++
	repository.input, repository.task = input, task
	return workqueue.Change{}, workexecution.Item{ID: input.WorkItemID, TaskID: task.ID, Input: &task.Intent}, nil
}

type fakeConnectorDeletionIdempotency struct {
	locator            testidempotency.IdempotencyLocator
	indexed            bool
	existing           bool
	existingResolution idempotentintent.Resolution
	known              idempotentintent.Resolution
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveReplayLocator(
	_ context.Context,
	_ testidempotency.IdempotencyReplayTarget,
	_ string,
	_ string,
	_ string,
) (testidempotency.IdempotencyLocator, bool, error) {
	return idempotency.locator, idempotency.indexed, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) Prepare(
	_ context.Context,
	_ testidempotency.IdempotencyLocator,
	_ string,
	_ []string,
) (connectorDeletionEvidence, error) {
	return connectorDeletionEvidence{}, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveExisting(
	_ context.Context,
	_ testidempotency.IdempotencyLocator,
	_ connectorDeletionEvidence,
) (idempotentintent.Resolution, bool, error) {
	return idempotency.existingResolution, idempotency.existing, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveKnown(
	_ context.Context,
	_ connectorDeletionEvidence,
	_ etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}

func (idempotency *fakeConnectorDeletionIdempotency) ResolveUnknown(
	_ context.Context,
	_ testidempotency.IdempotencyLocator,
	_ connectorDeletionEvidence,
	_ error,
) (idempotentintent.Resolution, error) {
	return idempotency.known, nil
}
