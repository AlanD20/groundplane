package serviceread

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type fakeServiceReadRepository struct {
	Environments
	Services
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	projection  testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
	page        testkeyvalue.Page[testservices.ServiceRecord]
	wantRequest testkeyvalue.PageRequest
	listed      bool
}

func (fake *fakeServiceReadRepository) GetEnvironmentComposeProjection(
	context.Context, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return fake.projection, fake.projection.Record.RevisionID != "", nil
}

func (fake *fakeServiceReadRepository) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return fake.environment, nil
}

func (fake *fakeServiceReadRepository) ListServices(
	_ context.Context,
	environmentID string,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testservices.ServiceRecord], error) {
	fake.listed = environmentID == fake.environment.Record.ID && request == fake.wantRequest
	return fake.page, nil
}

// Rationale: Service labels are scoped to an Environment, so a collection read must verify that
// owner and preserve the repository's opaque cursor tuple instead of treating a missing owner as empty.
func TestServiceListVerifiesOwnerAndPreservesPagination(t *testing.T) {
	t.Parallel()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	request := testkeyvalue.PageRequest{Limit: 23, Cursor: "opaque-cursor"}
	want := testkeyvalue.Page[testservices.ServiceRecord]{NextCursor: "next-cursor", Revision: 91}
	repository := &fakeServiceReadRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{
				NetworkPool: "10.40.0.0/16",
				ID:          environmentID,
			},
			Revision:     12,
			ReadRevision: 12,
		},
		page: want, wantRequest: request,
	}
	service, err := New(repository, repository)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	got, err := service.ListServices(context.Background(), environmentID, request)
	if err != nil || got.NextCursor != want.NextCursor || got.Revision != want.Revision ||
		len(got.Items) != len(want.Items) ||
		!repository.listed {
		t.Fatalf("ListServices() = %#v, %v, listed %t", got, err, repository.listed)
	}
}

func TestServiceDetailProjectsCanonicalNativeComposeFromDesiredHead(t *testing.T) {
	// Rationale: operator detail must read the immutable desired revision so
	// native Compose fields never depend on the intentionally smaller flat record.
	t.Parallel()
	at := time.Date(2026, time.August, 28, 10, 0, 0, 0, time.UTC)
	projectID := ids.NewAt(ids.KindProject, at, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 3)
	revisionID := ids.NewAt(ids.KindTask, at, 4)
	canonical := []byte(`services:
  api:
    image: example/api:1
    command: [serve, --http]
    environment: {APP_ENV: production}
    labels:
      example.role: api
      com.groundplane.managed: "true"
    annotations:
      example.note: retained
      com.groundplane.render-generation: "1"
    x-gp-resource: {kind: service, id: svc_generated}
`)
	digest := sha256.Sum256(canonical)
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&agentpb.ComposeArtifact{
		ArtifactId: ids.NewAt(ids.KindConfig, at, 5),
		OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:    environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
		CanonicalYaml: canonical, YamlSha256: digest[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	repository := &fakeServiceReadRepository{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{Record: testhierarchy.EnvironmentRecord{
			ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.40.0.0/16",
		}},
		projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
				ComposeArtifact: artifact,
			},
		},
	}
	service, err := New(repository, repository)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	native, err := service.GetServiceNativeCompose(context.Background(), environmentID, "api")
	if err != nil {
		t.Fatalf("GetServiceNativeCompose() error = %v", err)
	}
	for _, fragment := range []string{
		"services:", "api:", "image: example/api:1", "APP_ENV: production", "example.role: api", "example.note: retained",
	} {
		if !strings.Contains(native, fragment) {
			t.Fatalf("native Compose = %q, missing %q", native, fragment)
		}
	}
	for _, fragment := range []string{"com.groundplane.", "x-gp-resource"} {
		if strings.Contains(native, fragment) {
			t.Fatalf("native Compose = %q, contains Controller-owned %q", native, fragment)
		}
	}
}
