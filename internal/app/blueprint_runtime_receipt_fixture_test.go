package app

import (
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// AcknowledgedRuntime reads the actual persisted result of the public publisher
// and Task acknowledgement used by the external integration fixture.
func (fixture *ExecutedArtifactFixture) AcknowledgedRuntime(
	t *testing.T, serviceID string,
) testkeyvalue.Versioned[serviceruntimerecord.Record] {
	t.Helper()
	value, err := fixture.store.Get(t.Context(), serviceruntimerecord.Key(serviceID))
	if err != nil || value.Entry == nil {
		t.Fatalf("successful Blueprint has no acknowledged Service runtime: %v", err)
	}
	record, err := testreleases.DecodeReleaseRecord[serviceruntimerecord.Record](
		value.Entry.Value,
		"service-acknowledged-runtime",
	)
	if err != nil || record.EnvironmentID != fixture.Environment.Record.ID || record.Runtime.ServiceID != serviceID ||
		serviceruntimerecord.Validate(record) != nil {
		t.Fatal(errs.New(errs.KindInternal, "acknowledged Service runtime is corrupt"))
	}
	return testkeyvalue.Versioned[serviceruntimerecord.Record]{Record: record, Revision: value.Entry.ModRevision,
		ReadRevision: value.ReadRevision}
}
