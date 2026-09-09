package volume

import (
	"crypto/sha256"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

// Rationale: DELETE must seal its actual accepted consumer mounts, not fixture
// digests, while leaving the read-only source projection unchanged.
func TestVolumeRemovalEvidenceUsesDesiredMounts(t *testing.T) {
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	runtime := removal.Runtime{
		OperationID: ids.NewAt(ids.KindOperation, at, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 2),
		VolumeID: ids.NewAt(ids.KindVolume, at, 3), Key: "data", DesiredRevisionID: ids.NewAt(ids.KindTask, at, 4),
		ImpactSHA256: sha256.Sum256([]byte("accepted impact")),
	}
	serviceID := ids.NewAt(ids.KindService, at, 5)
	otherVolumeID := ids.NewAt(ids.KindVolume, at, 6)
	source := etcd.Versioned[etcd.EnvironmentComposeProjection]{Revision: 10, ReadRevision: 12,
		Record: etcd.EnvironmentComposeProjection{
			EnvironmentID: runtime.EnvironmentID,
			RevisionID:    ids.NewAt(ids.KindTask, at, 7),
			Volumes:       []etcd.EnvironmentVolumeIdentity{{ID: runtime.VolumeID, Key: runtime.Key, Slug: "data"}},
			VolumeMounts: []etcd.EnvironmentServiceVolumeMount{
				{ServiceID: serviceID, VolumeID: runtime.VolumeID, Target: "/z", ReadOnly: true},
				{ServiceID: serviceID, VolumeID: otherVolumeID, Target: "/unrelated"},
				{ServiceID: serviceID, VolumeID: runtime.VolumeID, Target: "/a"},
			},
		},
	}
	original := append([]etcd.EnvironmentServiceVolumeMount(nil), source.Record.VolumeMounts...)
	manifest, rows, err := volumeRemovalEvidence(runtime, source)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].MountTarget != "/a" || rows[1].MountTarget != "/z" || !rows[1].ReadOnly ||
		manifest.ReadRevision != 12 || manifest.SourceRevisionID != source.Record.RevisionID || manifest.TotalRows != 2 {
		t.Fatal("evidence is not the exact sorted source mounts")
	}
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		cursor, err = removal.AdvanceEvidenceCursor(cursor, manifest, row)
		if err != nil {
			t.Fatal(err)
		}
	}
	if cursor.CompletedRows != 2 || cursor.RollingSHA256 != manifest.OrderedSHA256 ||
		!slices.Equal(original, source.Record.VolumeMounts) {
		t.Fatal("evidence digest or source immutability changed")
	}
	source.Record.VolumeMounts = nil
	empty, rows, err := volumeRemovalEvidence(runtime, source)
	if err != nil || len(rows) != 0 || empty.OrderedSHA256 != removal.EmptyEvidenceDigest() {
		t.Fatal("empty consumer set", err)
	}
	source.Record.Volumes = nil
	if _, _, err := volumeRemovalEvidence(runtime, source); err == nil {
		t.Fatal("absent source Volume authorized evidence")
	}
}
