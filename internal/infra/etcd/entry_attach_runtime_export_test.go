package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
)

// SeedEntryRuntimeAttach models a completed Attach after the immutable Release
// was published. The desired Blueprint and serving Release stay unchanged.
func (fixture *ExecutedArtifactFixture) SeedEntryRuntimeAttach(t *testing.T, serviceID string) string {
	t.Helper()
	id, networkID := ids.New(ids.KindAttach), ids.New(ids.KindNetwork)
	record, err := NewPendingAttachRecord(id, fixture.Environment.Record.ID, "current-cache",
		ids.New(ids.KindProject), ids.New(ids.KindEnvironment), ids.New(ids.KindService), networkID,
		serviceID, id, nil, []AttachFactSetMetadata{{Facts: []AttachFactDefinition{{Key: "ENDPOINT"}}}},
		ids.New(ids.KindTask), taskJournalTime())
	if err != nil {
		t.Fatal(err)
	}
	record.Status = core.AttachReady
	value, err := encodeAttachRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Transact(t.Context(), nil, []Mutation{
		{Type: MutationPut, Key: attachKey(id), Value: value},
		{Type: MutationPut, Key: attachOwnerKey(record.EnvironmentID, id), Value: []byte(id)},
	}); err != nil {
		t.Fatal(err)
	}
	fixture.AdvanceEntryRuntimeEpoch(t)
	return networkID
}
