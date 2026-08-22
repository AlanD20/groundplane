package etcd

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestEnvironmentComposeProjectionPinsSortedRouteIdentities(t *testing.T) {
	// Rationale: restart-safe Caddy rendering must recover the stable Route IDs
	// assigned at apply without consulting mutable Route records.
	t.Parallel()
	now := time.Date(2026, 8, 22, 22, 0, 0, 0, time.UTC)
	projection := EnvironmentComposeProjection{
		EnvironmentID:       ids.NewAt(ids.KindEnvironment, now, 1),
		BlueprintRevisionID: ids.NewAt(ids.KindTask, now, 2),
		RenderGeneration:    1,
		Routes: []EnvironmentRouteIdentity{
			{ID: ids.NewAt(ids.KindRoute, now, 3), Host: "api.example.com", Path: "/"},
			{ID: ids.NewAt(ids.KindRoute, now, 4), Host: "app.example.com", Path: "/app/*"},
		},
	}
	encoded, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	decoded, err := decodeEnvironmentComposeProjection(encoded)
	if err != nil || len(decoded.Routes) != 2 || decoded.Routes[1].ID != projection.Routes[1].ID {
		t.Fatalf("decodeEnvironmentComposeProjection() = %#v, %v", decoded, err)
	}
	projection.Routes[0], projection.Routes[1] = projection.Routes[1], projection.Routes[0]
	if _, err := encodeEnvironmentComposeProjection(projection); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("encodeEnvironmentComposeProjection(unsorted Routes) error = %v", err)
	}
}
