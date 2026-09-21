package services

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// SVC-04: direct Service creation leaves optional runtime files absent; a
// later removal must preserve that exact projection shape while sealing its
// durable candidate intent.
func TestServiceRemovalCandidatePreservesAbsentRuntimeFiles(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, time.September, 20, 12, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, createdAt, 1)
	projectID := ids.NewAt(ids.KindProject, createdAt, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, createdAt, 3)
	serviceID := ids.NewAt(ids.KindService, createdAt, 4)
	currentRevisionID := ids.NewAt(ids.KindTask, createdAt, 5)
	candidateRevisionID := ids.NewAt(ids.KindTask, createdAt, 6)
	environment := testhierarchy.EnvironmentRecord{
		ID: environmentID, ProjectID: projectID,
		VolumeDir: "/var/lib/groundplane/vol/test/" + environmentID,
	}
	record, err := testservices.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "log-emitter", Image: "example/log-emitter:1",
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureLeaveActive, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	current, err := buildServiceDesiredProjection(
		tenantID,
		projectID,
		environment, testenvironmentprojection.EnvironmentComposeProjection{}, false,
		record, testservices.ServiceMutationReferences{}, true,
		currentRevisionID,
		1,
	)
	if err != nil {
		t.Fatalf("buildServiceDesiredProjection() error = %v", err)
	}
	candidate, err := buildServiceRemovalProjection(
		tenantID,
		projectID,
		current,
		record,
		candidateRevisionID,
		2,
	)
	if err != nil {
		t.Fatalf("buildServiceRemovalProjection() error = %v", err)
	}
	claim := testblueprints.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(candidateRevisionID, "task_"),
		EnvironmentID: environmentID, RevisionID: candidateRevisionID, TaskID: candidateRevisionID,
		Locator: testidempotency.IdempotencyLocator{
			ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: http.MethodDelete, Route: "/services/{id}", Key: "remove-log-emitter-0001",
		},
		Intent:               serviceTestProtectedIntent(),
		BaselineHeadRevision: 11, SourceKind: testblueprints.EnvironmentBlueprintSourceMutation,
		RenderGeneration: 2, ProjectionSchema: testblueprints.EnvironmentDesiredProjectionSchema, CreatedAt: createdAt,
	}
	intent, err := testenvironmentchanges.NewServiceRemovalIntent(
		candidateRevisionID,
		testkeyvalue.Versioned[testservices.ServiceRecord]{Record: record, Revision: 12, ReadRevision: 12},
		testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record:       current,
			Revision:     11,
			ReadRevision: 12,
		},
		11,
		claim,
		candidate,
		createdAt,
	)
	if err != nil {
		t.Fatalf("NewServiceRemovalIntent() error = %v", err)
	}
	if intent.ServiceID != serviceID || intent.CandidateProjection.RevisionID != candidateRevisionID {
		t.Fatalf("removal intent = %#v", intent)
	}
}
