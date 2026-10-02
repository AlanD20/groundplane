package backupvolumemanifest

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Complete struct {
	Start    *agentpb.BackupVolumeManifestStart
	Entries  []backupvolume.Entry
	Manifest []backupvolume.ManifestEntryBytes
	Archive  backupvolume.ArchiveEvidence
	Source   *backupformat.Evidence
	Revision int64
}

// ReadComplete reconstructs every frame at one fixed MVCC revision. A cursor
// alone, or a page that omits a native frame, never proves a complete tree.
func (repository *Repository) ReadComplete(ctx context.Context, owner Owner,
	revision int64,
) (Complete, error) {
	cursor, found, err := repository.Read(ctx, owner, revision)
	if err != nil {
		return Complete{}, err
	}
	if !found || !cursor.Record.Complete {
		return Complete{}, invalidLedger()
	}
	last, err := cursor.Record.LastCredit()
	if err != nil {
		return Complete{}, err
	}
	seed, err := backupvolumetransfer.InitialChain(owner.Binding)
	if err != nil {
		return Complete{}, err
	}
	chain := seed
	var start *agentpb.BackupVolumeManifestStart
	var end *agentpb.BackupVolumeManifestEnd
	var wires []*agentpb.BackupVolumeManifestEntry
	var sequence uint64
	var next uint64 = 1
	pageStart := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix(owner.Binding) + "frame/", StartExclusive: pageStart,
			Revision: cursor.ReadRevision, Limit: 32})
		if err != nil {
			return Complete{}, err
		}
		if page == nil || page.ReadRevision != cursor.ReadRevision || len(page.Values) > 32 {
			return Complete{}, invalidLedger()
		}
		for _, value := range page.Values {
			if sequence >= uint64(
				2+(backupvolume.MaxEntries+int(backupvolumetransfer.MaxBatchEntries)-1)/int(
					backupvolumetransfer.MaxBatchEntries,
				),
			) {
				return Complete{}, invalidLedger()
			}
			sequence++
			storedOwner, frame, err := decodeFrame(value.Value)
			clear(value.Value)
			if err != nil || storedOwner != owner || value.Key != FrameKey(owner.Binding, sequence) ||
				value.Version != 1 || value.ModRevision <= 0 || value.ModRevision > cursor.Revision ||
				frame.RecordSequence != sequence {
				return Complete{}, invalidLedger()
			}
			chain, err = backupvolumetransfer.NextChain(owner.Binding, chain, frame)
			if err != nil {
				return Complete{}, err
			}
			switch {
			case frame.GetStart() != nil:
				if start != nil || sequence != 1 {
					return Complete{}, invalidLedger()
				}
				start = frame.GetStart()
			case frame.GetBatch() != nil:
				batch := frame.GetBatch()
				if start == nil || end != nil || batch.FirstOrdinal != next ||
					next+uint64(len(batch.Entries))-1 > start.EntryCount {
					return Complete{}, invalidLedger()
				}
				wires = append(wires, batch.Entries...)
				next += uint64(len(batch.Entries))
			case frame.GetEnd() != nil:
				if start == nil || end != nil || next != start.EntryCount+1 {
					return Complete{}, invalidLedger()
				}
				end = frame.GetEnd()
			default:
				return Complete{}, invalidLedger()
			}
			pageStart = value.Key
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return Complete{}, invalidLedger()
		}
	}
	if start == nil || end == nil || sequence != last.CommittedRecordSequence ||
		last.NextOrdinal != next || !bytes.Equal(last.TransferChainSha256, chain[:]) ||
		start.EntryCount != uint64(len(wires)) || end.EntryCount != start.EntryCount ||
		!bytes.Equal(end.ContentManifestSha256, start.ContentManifestSha256) ||
		!bytes.Equal(end.FullTreeSha256, start.FullTreeSha256) ||
		!bytes.Equal(cursor.Record.ContentSHA, start.ContentManifestSha256) ||
		!bytes.Equal(cursor.Record.FullTreeSHA, start.FullTreeSha256) ||
		cursor.Record.PointID != start.PointId || cursor.Record.Generation != start.RestoreGenerationId {
		return Complete{}, invalidLedger()
	}
	entries, manifest, err := backupvolumetransfer.DecodeEntries(wires)
	if err != nil {
		return Complete{}, err
	}
	archive := &agentpb.BackupVolumeArchiveEvidence{EntryCount: start.EntryCount,
		ContentManifestSha256: start.ContentManifestSha256, FullTreeSha256: start.FullTreeSha256}
	if source := start.GetSourceArchive(); source != nil {
		archive.SourceSizeBytes = source.SourceSizeBytes
	}
	if err := backupvolumetransfer.VerifyEntries(entries, manifest, archive); err != nil {
		return Complete{}, err
	}
	value, err := backupvolumetransfer.ArchiveEvidenceFromWire(archive)
	if err != nil {
		return Complete{}, err
	}
	result := Complete{Start: start, Entries: entries, Manifest: manifest, Archive: value, Revision: cursor.Revision}
	if source := start.GetSourceArchive(); source != nil {
		if len(source.SourceSha256) != sha256.Size {
			return Complete{}, invalidLedger()
		}
		result.Source = &backupformat.Evidence{SizeBytes: source.SourceSizeBytes}
		copy(result.Source.SHA256[:], source.SourceSha256)
		if result.Source.SizeBytes != result.Archive.SourceSizeBytes {
			return Complete{}, invalidLedger()
		}
	}
	return result, nil
}
