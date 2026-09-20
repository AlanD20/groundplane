package app

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
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
	environment := etcd.EnvironmentRecord{
		ID: environmentID, ProjectID: projectID,
		VolumeDir: "/var/lib/groundplane/vol/test/" + environmentID,
	}
	record, err := etcd.NewServiceRecord(environmentID, core.Service{
		ID: serviceID, Name: "log-emitter", Image: "example/log-emitter:1",
		Strategy: core.StrategyRecreate, OnFailure: core.OnFailureLeaveActive, Replicas: 1,
	}, "")
	if err != nil {
		t.Fatal(err)
	}
	current, err := buildServiceDesiredProjection(
		tenantID,
		projectID,
		environment,
		etcd.EnvironmentComposeProjection{},
		false,
		record,
		etcd.ServiceMutationReferences{},
		true,
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
	claim := etcd.EnvironmentBlueprintStageClaim{
		DescriptorID:  strings.TrimPrefix(candidateRevisionID, "task_"),
		EnvironmentID: environmentID, RevisionID: candidateRevisionID, TaskID: candidateRevisionID,
		Locator: etcd.IdempotencyLocator{
			ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
			Method: http.MethodDelete, Route: "/services/{id}", Key: "remove-log-emitter-0001",
		},
		Intent:               projectCreationTestEvidence().durable,
		BaselineHeadRevision: 11, SourceKind: etcd.EnvironmentBlueprintSourceMutation,
		RenderGeneration: 2, ProjectionSchema: etcd.EnvironmentDesiredProjectionSchema, CreatedAt: createdAt,
	}
	intent, err := etcd.NewServiceRemovalIntent(
		candidateRevisionID,
		etcd.Versioned[etcd.ServiceRecord]{Record: record, Revision: 12, ReadRevision: 12},
		etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: current, Revision: 11, ReadRevision: 12},
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
