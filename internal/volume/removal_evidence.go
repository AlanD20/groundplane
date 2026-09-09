package volume

import (
	"cmp"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// volumeRemovalEvidence materializes only the accepted Volume's mount intent.
// The desired projection remains the source of truth; these rows authorize no
// execution until the existing staging, sealing and publication path accepts it.
func volumeRemovalEvidence(runtime removal.Runtime, source etcd.Versioned[etcd.EnvironmentComposeProjection]) (
	removal.EvidenceManifest, []removal.EvidenceRow, error,
) {
	if source.Revision <= 0 || source.ReadRevision < source.Revision ||
		source.Record.EnvironmentID != runtime.EnvironmentID {
		return removal.EvidenceManifest{}, nil, errs.New(
			errs.KindStateConflict,
			"Volume removal source revision is invalid",
		)
	}
	found := false
	for _, volume := range source.Record.Volumes {
		if volume.ID == runtime.VolumeID && volume.Key == runtime.Key {
			found = true
			break
		}
	}
	if !found {
		return removal.EvidenceManifest{}, nil, errs.New(
			errs.KindStateConflict,
			"Volume removal source identity changed",
		)
	}
	var mounts []etcd.EnvironmentServiceVolumeMount
	for _, mount := range source.Record.VolumeMounts {
		if mount.VolumeID == runtime.VolumeID {
			mounts = append(mounts, mount)
		}
	}
	slices.SortFunc(mounts, func(left, right etcd.EnvironmentServiceVolumeMount) int {
		if order := cmp.Compare(left.ServiceID, right.ServiceID); order != 0 {
			return order
		}
		return cmp.Compare(left.Target, right.Target)
	})
	manifest := removal.EvidenceManifest{
		OperationID: runtime.OperationID, EnvironmentID: runtime.EnvironmentID, VolumeID: runtime.VolumeID, Key: runtime.Key,
		ReadRevision: source.ReadRevision, SourceRevisionID: source.Record.RevisionID, DesiredRevisionID: runtime.DesiredRevisionID,
		ImpactSHA256: runtime.ImpactSHA256, TotalRows: uint64(len(mounts)), MaximumOrdinal: uint64(len(mounts)),
		OrderedSHA256: removal.EmptyEvidenceDigest(),
	}
	rows := make([]removal.EvidenceRow, len(mounts))
	for index, mount := range mounts {
		if index > 0 && mounts[index-1].ServiceID == mount.ServiceID && mounts[index-1].Target == mount.Target {
			return removal.EvidenceManifest{}, nil, errs.New(
				errs.KindStateConflict,
				"Volume removal source mount is duplicated",
			)
		}
		row := removal.EvidenceRow{
			OperationID: runtime.OperationID, VolumeID: runtime.VolumeID, ConsumerID: mount.ServiceID,
			SourceRevisionID: source.Record.RevisionID, Ordinal: uint64(index + 1), MountTarget: mount.Target, ReadOnly: mount.ReadOnly,
		}
		var err error
		row.SHA256, err = removal.EvidenceRowDigest(row)
		if err != nil {
			return removal.EvidenceManifest{}, nil, err
		}
		rows[index] = row
		manifest.OrderedSHA256 = removal.AppendEvidenceDigest(manifest.OrderedSHA256, row.SHA256)
	}
	value, err := removal.EncodeEvidenceManifest(manifest)
	clear(value)
	if err != nil {
		return removal.EvidenceManifest{}, nil, err
	}
	return manifest, rows, nil
}
