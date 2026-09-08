package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckManualJourneyServiceRemoval exercises the actual removal preflight
// against the Service and source memberships published by the journey.
func (fixture *ExecutedArtifactFixture) CheckManualJourneyServiceRemoval(t *testing.T, serviceID string, active bool) {
	t.Helper()
	ctx := context.Background()
	services, err := newServiceRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	current, err := services.GetService(ctx, serviceID)
	if err != nil {
		t.Fatal(err)
	}
	projection, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, fixture.Environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("Service removal desired projection = %t, %v", found, err)
	}
	before := fixture.store.revision
	err = services.ValidateServiceRemovalReferences(ctx, current, projection)
	if active && !isKind(err, errs.KindResourceInUse) {
		t.Fatalf("Service removal ignored retained Script source: %v", err)
	}
	if !active && err != nil {
		t.Fatalf("Service removal remained blocked after Script cleanup: %v", err)
	}
	if fixture.store.revision != before {
		t.Fatal("Service removal preflight mutated source authority")
	}
}
