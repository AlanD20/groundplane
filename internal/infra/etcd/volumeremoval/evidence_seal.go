package volumeremoval

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EvidenceSealResult struct {
	State    EvidenceState
	Seal     etcd.Versioned[removal.EvidenceSeal]
	Replayed bool
}

// Seal verifies every stored row and rejects extra namespace records at one
// fixed revision, using at most 44 values per page. Stage cannot change rows
// after cursor completion. Any future cleanup must invalidate the manifest or
// cursor before deleting rows, and fence publication before removing a seal.
func (repository *EvidenceRepository) Seal(
	ctx context.Context,
	manifest removal.EvidenceManifest,
) (EvidenceSealResult, error) {
	state, found, err := repository.read(ctx, manifest)
	if err != nil {
		return EvidenceSealResult{}, err
	}
	if !found || state.Cursor.Record.CompletedRows != manifest.TotalRows {
		return EvidenceSealResult{}, evidenceConflict()
	}
	seal, err := repository.verifyEvidenceRows(ctx, state)
	if err != nil {
		return EvidenceSealResult{}, err
	}
	if seal.Revision != 0 {
		return EvidenceSealResult{State: state, Seal: seal, Replayed: true}, nil
	}
	record := removal.EvidenceSeal{
		ManifestSHA256:   state.Cursor.Record.ManifestSHA256,
		ManifestRevision: state.Manifest.Revision, CursorRevision: state.Cursor.Revision,
		VerifiedRevision: state.Cursor.ReadRevision,
	}
	value, err := removal.EncodeEvidenceSeal(record, manifest)
	if err != nil {
		return EvidenceSealResult{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: removal.EvidenceManifestKey(manifest.OperationID), ModRevision: state.Manifest.Revision},
		{Key: removal.EvidenceCursorKey(manifest.OperationID), ModRevision: state.Cursor.Revision},
		{Key: removal.EvidenceSealKey(manifest.OperationID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: removal.EvidenceSealKey(manifest.OperationID), Value: value},
	}
	size, err := repository.store.VolumeRemovalEvidenceTransactionSize(conditions, mutations)
	if err != nil {
		return EvidenceSealResult{}, err
	}
	if size > removal.EvidenceTransactionBytes {
		return EvidenceSealResult{}, errs.New(errs.KindInternal, "volume removal evidence seal exceeds its budget")
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return EvidenceSealResult{}, err
	}
	if !result.Succeeded {
		clearKeyValues(result.FailureReads)
		return EvidenceSealResult{}, evidenceConflict()
	}
	return EvidenceSealResult{
		State: state,
		Seal: etcd.Versioned[removal.EvidenceSeal]{
			Record:       record,
			Revision:     result.Revision,
			ReadRevision: result.Revision,
		},
	}, nil
}

func (repository *EvidenceRepository) verifyEvidenceRows(
	ctx context.Context,
	state EvidenceState,
) (etcd.Versioned[removal.EvidenceSeal], error) {
	manifest := state.Manifest.Record
	manifestValue, err := removal.EncodeEvidenceManifest(manifest)
	if err != nil {
		return etcd.Versioned[removal.EvidenceSeal]{}, err
	}
	defer clear(manifestValue)
	cursorValue, err := removal.EncodeEvidenceCursor(state.Cursor.Record, manifest)
	if err != nil {
		return etcd.Versioned[removal.EvidenceSeal]{}, err
	}
	defer clear(cursorValue)
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		return etcd.Versioned[removal.EvidenceSeal]{}, err
	}
	var seal etcd.Versioned[removal.EvidenceSeal]
	seenManifest, seenCursor := false, false
	request := etcdstore.RangeRequest{
		Prefix:   removal.EvidenceRoot(manifest.OperationID),
		Limit:    removal.EvidenceBatchRows,
		Revision: state.Cursor.ReadRevision,
	}
	for {
		page, err := repository.store.Range(ctx, request)
		if err != nil {
			return etcd.Versioned[removal.EvidenceSeal]{}, err
		}
		if page == nil {
			return etcd.Versioned[removal.EvidenceSeal]{}, evidenceConflict()
		}
		err = func() error {
			defer func() {
				for _, entry := range page.Values {
					clear(entry.Value)
				}
			}()
			if page.ReadRevision != request.Revision || len(page.Values) > removal.EvidenceBatchRows ||
				(page.More && len(page.Values) == 0) {
				return evidenceConflict()
			}
			for _, entry := range page.Values {
				if entry.Key <= request.StartExclusive || entry.ModRevision <= 0 ||
					entry.ModRevision > request.Revision {
					return evidenceConflict()
				}
				request.StartExclusive = entry.Key
				switch entry.Key {
				case removal.EvidenceCursorKey(manifest.OperationID):
					if seenCursor || entry.ModRevision != state.Cursor.Revision ||
						!bytes.Equal(entry.Value, cursorValue) {
						return evidenceConflict()
					}
					seenCursor = true
				case removal.EvidenceManifestKey(manifest.OperationID):
					if !seenCursor || seenManifest || entry.ModRevision != state.Manifest.Revision ||
						!bytes.Equal(entry.Value, manifestValue) {
						return evidenceConflict()
					}
					seenManifest = true
				case removal.EvidenceSealKey(manifest.OperationID):
					if !seenManifest || cursor != state.Cursor.Record || seal.Revision != 0 {
						return evidenceConflict()
					}
					record, err := removal.DecodeEvidenceSeal(entry.Value, manifest)
					if err != nil {
						return err
					}
					if record.ManifestRevision != state.Manifest.Revision ||
						record.CursorRevision != state.Cursor.Revision ||
						record.VerifiedRevision >= entry.ModRevision ||
						entry.ModRevision <= state.Cursor.Revision {
						return evidenceConflict()
					}
					seal = etcd.Versioned[removal.EvidenceSeal]{
						Record:       record,
						Revision:     entry.ModRevision,
						ReadRevision: request.Revision,
					}
				default:
					if !seenManifest || seal.Revision != 0 ||
						entry.Key != removal.EvidenceRowKey(manifest.OperationID, cursor.NextOrdinal) ||
						entry.ModRevision < state.Manifest.Revision ||
						entry.ModRevision > state.Cursor.Revision {
						return evidenceConflict()
					}
					row, err := removal.DecodeEvidenceRow(entry.Value)
					if err != nil {
						return err
					}
					cursor, err = removal.AdvanceEvidenceCursor(cursor, manifest, row)
					if err != nil {
						return err
					}
				}
			}
			return nil
		}()
		if err != nil {
			return etcd.Versioned[removal.EvidenceSeal]{}, err
		}
		if !page.More {
			break
		}
	}
	if !seenManifest || !seenCursor || cursor != state.Cursor.Record {
		return etcd.Versioned[removal.EvidenceSeal]{}, evidenceConflict()
	}
	return seal, nil
}
