package app

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A selected PostgreSQL tag must control database bytes independently of the
// installed backup tools. A retry after tag movement must not choose new bytes.
func TestNativePostgresCreationHonorsImageSelection(t *testing.T) {
	f := newNativeVolumeFixture(t)
	service, images, _ := newNativeBackingCreationService(t, f)
	images.plan.Requested = "postgres:16-alpine"
	images.plan.Repository = "docker.io/library/postgres"
	request := api.BackingServiceCreate{Slug: "selected-postgres", Name: "Selected PostgreSQL",
		Adapter: "postgres", AdapterVersion: "16", Image: images.plan.Requested,
		NetworkPool: "10.205.0.0/24", Zone: api.BackingServiceZoneCreate{Name: "data", Subnet: "10.205.0.0/28", Internal: true}}
	response, err := service.CreateBackingService(t.Context(), request, "native-postgres-selected-image")
	if err != nil || response.Status != 201 {
		t.Fatalf("creation: %v, status %d", err, response.Status)
	}
	var created api.BackingServiceCreated
	if err := json.Unmarshal(response.Body, &created); err != nil {
		t.Fatal(err)
	}
	repository, err := etcd.NewServiceRepository(f.store)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := repository.GetService(t.Context(), created.BackingService.ServiceID)
	if err != nil {
		t.Fatal(err)
	}
	want := request.Image + "@" + images.plan.ManifestDigest
	if stored.Record.Desired.Image != want || stored.Record.Runtime.PostgresToolsImage == "" {
		t.Fatalf(
			"database image %q, want %q; backup tools %q",
			stored.Record.Desired.Image,
			want,
			stored.Record.Runtime.PostgresToolsImage,
		)
	}
	images.plan.ManifestDigest = "sha256:" + strings.Repeat("c", 64)
	_, err = service.CreateBackingService(t.Context(), request, "native-postgres-selected-image")
	if !errors.Is(err, errs.New(errs.KindIdempotencyInProgress, "")) || images.registryCalls != 1 ||
		images.agentCalls != 1 {
		t.Fatal("retry resolved a moved tag or changed captured image", err)
	}
	request.Slug, request.Image = "wrong-version", "postgres:17-alpine"
	_, err = service.CreateBackingService(t.Context(), request, "native-postgres-invalid-version")
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || images.registryCalls != 1 {
		t.Fatal("incompatible server version reached image resolution", err)
	}
}
