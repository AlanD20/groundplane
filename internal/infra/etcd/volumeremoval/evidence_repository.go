package volumeremoval

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type evidenceStore interface {
	GetMany(context.Context, etcd.GetManyRequest) (*etcd.GetManyResult, error)
	Range(context.Context, etcd.RangeRequest) (*etcd.RangeResult, error)
	Transact(context.Context, []etcd.Condition, []etcd.Mutation) (etcd.TransactionResult, error)
	VolumeRemovalEvidenceTransactionSize([]etcd.Condition, []etcd.Mutation) (int, error)
}

// EvidenceRepository owns only bounded private staging. A completed cursor is
// not a publication seal and does not confer desired-state or execution authority.
type EvidenceRepository struct{ store evidenceStore }

type EvidenceState struct {
	Manifest etcd.Versioned[removal.EvidenceManifest]
	Cursor   etcd.Versioned[removal.EvidenceCursor]
}

type EvidenceStageResult struct {
	State        EvidenceState
	AcceptedRows int
	Replayed     bool
}

func NewEvidenceRepository(backend evidenceStore) (*EvidenceRepository, error) {
	if backend == nil {
		return nil, errs.New(errs.KindInternal, "volume removal evidence store is required")
	}
	return &EvidenceRepository{store: backend}, nil
}

// Begin creates an immutable manifest with its empty cursor. Equal calls resume
// existing progress; an orphaned staging prefix cannot be silently adopted.
func (repository *EvidenceRepository) Begin(
	ctx context.Context,
	manifest removal.EvidenceManifest,
) (EvidenceState, error) {
	state, found, err := repository.read(ctx, manifest)
	if err != nil || found {
		return state, err
	}
	manifestValue, err := removal.EncodeEvidenceManifest(manifest)
	if err != nil {
		return EvidenceState{}, err
	}
	defer clear(manifestValue)
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		return EvidenceState{}, err
	}
	cursorValue, err := removal.EncodeEvidenceCursor(cursor, manifest)
	if err != nil {
		return EvidenceState{}, err
	}
	defer clear(cursorValue)
	conditions := []etcd.Condition{{Key: removal.EvidenceRoot(manifest.OperationID), Prefix: true}}
	mutations := []etcd.Mutation{
		{Type: etcd.MutationPut, Key: removal.EvidenceManifestKey(manifest.OperationID), Value: manifestValue},
		{Type: etcd.MutationPut, Key: removal.EvidenceCursorKey(manifest.OperationID), Value: cursorValue},
	}
	size, err := repository.store.VolumeRemovalEvidenceTransactionSize(conditions, mutations)
	if err != nil {
		return EvidenceState{}, err
	}
	if size > removal.EvidenceTransactionBytes {
		return EvidenceState{}, errs.New(errs.KindInternal, "volume removal evidence initialization exceeds its budget")
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return EvidenceState{}, err
	}
	if !result.Succeeded {
		clearKeyValues(result.FailureReads)
		state, found, err := repository.read(ctx, manifest)
		if err != nil {
			return EvidenceState{}, err
		}
		if !found {
			return EvidenceState{}, evidenceConflict()
		}
		return state, nil
	}
	return EvidenceState{
		Manifest: etcd.Versioned[removal.EvidenceManifest]{
			Record:       manifest,
			Revision:     result.Revision,
			ReadRevision: result.Revision,
		},
		Cursor: etcd.Versioned[removal.EvidenceCursor]{
			Record:       cursor,
			Revision:     result.Revision,
			ReadRevision: result.Revision,
		},
	}, nil
}

// Stage consumes the largest fitting prefix of the next full candidate window
// (up to 44 rows). A repeated request only acknowledges its already committed
// prefix; the caller then supplies a fresh window at the returned next ordinal.
func (repository *EvidenceRepository) Stage(
	ctx context.Context, manifest removal.EvidenceManifest, rows []removal.EvidenceRow,
) (EvidenceStageResult, error) {
	if len(rows) > removal.EvidenceBatchRows {
		return EvidenceStageResult{}, errs.New(errs.KindValidationFailed, "volume removal evidence window is oversized")
	}
	state, found, err := repository.read(ctx, manifest)
	if err != nil {
		return EvidenceStageResult{}, err
	}
	if !found {
		return EvidenceStageResult{}, evidenceConflict()
	}
	if len(rows) == 0 {
		if state.Cursor.Record.CompletedRows != manifest.TotalRows {
			return EvidenceStageResult{}, evidenceConflict()
		}
		return EvidenceStageResult{State: state, Replayed: true}, nil
	}
	first := rows[0].Ordinal
	values := make([][]byte, len(rows))
	defer func() {
		for _, value := range values {
			clear(value)
		}
	}()
	keys := make([]string, len(rows))
	for index, row := range rows {
		if row.Ordinal != first+uint64(index) || row.Ordinal < first || row.Ordinal > manifest.MaximumOrdinal ||
			row.OperationID != manifest.OperationID || row.VolumeID != manifest.VolumeID || row.SourceRevisionID != manifest.SourceRevisionID {
			return EvidenceStageResult{}, evidenceConflict()
		}
		values[index], err = removal.EncodeEvidenceRow(row)
		if err != nil {
			return EvidenceStageResult{}, err
		}
		keys[index] = removal.EvidenceRowKey(manifest.OperationID, row.Ordinal)
	}
	read, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: keys, Revision: state.Cursor.ReadRevision})
	if err != nil {
		return EvidenceStageResult{}, err
	}
	if read == nil || read.ReadRevision != state.Cursor.ReadRevision || len(read.Values) != len(keys) {
		return EvidenceStageResult{}, evidenceConflict()
	}
	defer clearKeyValues(read.Values)
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision ||
				!bytes.Equal(value.Value, values[index])) {
			return EvidenceStageResult{}, evidenceConflict()
		}
	}
	if first < state.Cursor.Record.NextOrdinal {
		count := int(min(uint64(len(rows)), state.Cursor.Record.NextOrdinal-first))
		for _, value := range read.Values[:count] {
			if value == nil {
				return EvidenceStageResult{}, evidenceConflict()
			}
		}
		return EvidenceStageResult{State: state, AcceptedRows: count, Replayed: true}, nil
	}
	remaining := manifest.TotalRows - state.Cursor.Record.CompletedRows
	if first != state.Cursor.Record.NextOrdinal || len(rows) != int(min(uint64(removal.EvidenceBatchRows), remaining)) {
		return EvidenceStageResult{}, evidenceConflict()
	}
	conditions := []etcd.Condition{
		{Key: removal.EvidenceManifestKey(manifest.OperationID), ModRevision: state.Manifest.Revision},
		{Key: removal.EvidenceCursorKey(manifest.OperationID), ModRevision: state.Cursor.Revision},
	}
	mutations := make([]etcd.Mutation, 0, len(rows))
	progress := make([]removal.EvidenceCursor, len(rows))
	putCounts := make([]int, len(rows))
	cursor := state.Cursor.Record
	for index, row := range rows {
		cursor, err = removal.AdvanceEvidenceCursor(cursor, manifest, row)
		if err != nil {
			return EvidenceStageResult{}, err
		}
		progress[index] = cursor
		condition := etcd.Condition{Key: keys[index]}
		if read.Values[index] == nil {
			mutations = append(mutations, etcd.Mutation{Type: etcd.MutationPut, Key: keys[index], Value: values[index]})
		} else {
			condition.ModRevision = read.Values[index].ModRevision
		}
		conditions = append(conditions, condition)
		putCounts[index] = len(mutations)
	}
	for count := len(rows); count > 0; count-- {
		cursorValue, err := removal.EncodeEvidenceCursor(progress[count-1], manifest)
		if err != nil {
			return EvidenceStageResult{}, err
		}
		writes := append([]etcd.Mutation(nil), mutations[:putCounts[count-1]]...)
		writes = append(
			writes,
			etcd.Mutation{
				Type:  etcd.MutationPut,
				Key:   removal.EvidenceCursorKey(manifest.OperationID),
				Value: cursorValue,
			},
		)
		size, err := repository.store.VolumeRemovalEvidenceTransactionSize(conditions[:count+2], writes)
		if err != nil {
			clear(cursorValue)
			return EvidenceStageResult{}, err
		}
		if size > removal.EvidenceTransactionBytes {
			clear(cursorValue)
			continue
		}
		result, err := repository.store.Transact(ctx, conditions[:count+2], writes)
		clear(cursorValue)
		if err != nil {
			return EvidenceStageResult{}, err
		}
		if !result.Succeeded {
			clearKeyValues(result.FailureReads)
			return EvidenceStageResult{}, evidenceConflict()
		}
		state.Manifest.ReadRevision = result.Revision
		state.Cursor = etcd.Versioned[removal.EvidenceCursor]{
			Record:       progress[count-1],
			Revision:     result.Revision,
			ReadRevision: result.Revision,
		}
		return EvidenceStageResult{State: state, AcceptedRows: count}, nil
	}
	return EvidenceStageResult{}, errs.New(errs.KindInternal, "one valid volume removal evidence row cannot fit")
}

func (repository *EvidenceRepository) read(
	ctx context.Context, manifest removal.EvidenceManifest,
) (EvidenceState, bool, error) {
	if err := etcd.ValidateCapabilityContext(ctx); err != nil {
		return EvidenceState{}, false, err
	}
	value, err := removal.EncodeEvidenceManifest(manifest)
	if err != nil {
		return EvidenceState{}, false, err
	}
	defer clear(value)
	keys := []string{removal.EvidenceManifestKey(manifest.OperationID), removal.EvidenceCursorKey(manifest.OperationID)}
	read, err := repository.store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
	if err != nil {
		return EvidenceState{}, false, err
	}
	if read == nil || read.ReadRevision <= 0 || manifest.ReadRevision > read.ReadRevision || len(read.Values) != 2 {
		return EvidenceState{}, false, evidenceConflict()
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil && read.Values[1] == nil {
		return EvidenceState{}, false, nil
	}
	for index, entry := range read.Values {
		if entry == nil || entry.Key != keys[index] || entry.ModRevision <= 0 || entry.ModRevision > read.ReadRevision {
			return EvidenceState{}, false, evidenceConflict()
		}
	}
	if !bytes.Equal(read.Values[0].Value, value) || read.Values[1].ModRevision < read.Values[0].ModRevision {
		return EvidenceState{}, false, evidenceConflict()
	}
	cursor, err := removal.DecodeEvidenceCursor(read.Values[1].Value, manifest)
	if err != nil {
		return EvidenceState{}, false, err
	}
	return EvidenceState{
		Manifest: etcd.Versioned[removal.EvidenceManifest]{
			Record:       manifest,
			Revision:     read.Values[0].ModRevision,
			ReadRevision: read.ReadRevision,
		},
		Cursor: etcd.Versioned[removal.EvidenceCursor]{
			Record:       cursor,
			Revision:     read.Values[1].ModRevision,
			ReadRevision: read.ReadRevision,
		},
	}, true, nil
}

func evidenceConflict() error {
	return errs.New(errs.KindStateConflict, "volume removal evidence changed or is incomplete")
}
