package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// AcknowledgedRuntime reads the actual persisted result of the public publisher
// and Task acknowledgement used by the external integration fixture.
func (fixture *ExecutedArtifactFixture) AcknowledgedRuntime(
	t *testing.T, serviceID string,
) Versioned[serviceruntimerecord.Record] {
	t.Helper()
	value, err := fixture.store.Get(t.Context(), serviceruntimerecord.Key(serviceID))
	if err != nil || value.Entry == nil {
		t.Fatalf("successful Blueprint has no acknowledged Service runtime: %v", err)
	}
	record, err := decodeAcknowledgedServiceRuntime(value.Entry.Value, fixture.Environment.Record.ID, serviceID)
	if err != nil {
		t.Fatal(err)
	}
	return Versioned[serviceruntimerecord.Record]{Record: record, Revision: value.Entry.ModRevision,
		ReadRevision: value.ReadRevision}
}
