package acceptedactions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) ReadManifest(ctx context.Context, taskID string, reference workexecution.Input,
	revision int64,
) (Snapshot, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || reference.Validate() != nil || revision <= 0 {
		return Snapshot{}, errs.New(errs.KindValidationFailed, "accepted action read requires a fixed Task snapshot")
	}
	key := Key(taskID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return Snapshot{}, errs.New(errs.KindStateConflict, "accepted action input is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
		return Snapshot{}, errs.New(errs.KindInternal, "accepted action input revision is invalid")
	}
	record, err := recordcodec.Decode[Record](value.Value, "accepted-action-input")
	if err != nil {
		return Snapshot{}, err
	}
	if record.Validate() != nil || !record.Sealed || record.TaskID != taskID || record.Reference != reference {
		return Snapshot{}, errs.New(
			errs.KindStateConflict,
			"accepted action input is unsealed or belongs to another Task",
		)
	}
	return Snapshot{Record: record, Revision: value.ModRevision}, nil
}

func (repository *Repository) Read(ctx context.Context, taskID string, reference workexecution.Input,
	kind string, revision int64,
) (Snapshot, error) {
	manifest, err := repository.ReadManifest(ctx, taskID, reference, revision)
	if err != nil {
		return Snapshot{}, err
	}
	record := manifest.Record
	if record.Kind != kind {
		return Snapshot{}, errs.New(errs.KindStateConflict, "accepted input belongs to another action")
	}
	keys := make([]string, record.Chunks)
	for index := range keys {
		keys[index] = chunkKey(taskID, uint16(index))
	}
	chunks, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return Snapshot{}, err
	}
	if chunks == nil || chunks.ReadRevision != revision || len(chunks.Values) != len(keys) {
		return Snapshot{}, errs.New(errs.KindInternal, "accepted action ciphertext capture is incomplete")
	}
	defer keyvalue.ClearValues(chunks.Values)
	ciphertext := make([]byte, 0, record.Bytes)
	for index, chunk := range chunks.Values {
		expected := min(ChunkBytes, int(record.Bytes)-index*ChunkBytes)
		if chunk == nil || chunk.Key != keys[index] || len(chunk.Value) != expected || chunk.ModRevision <= 0 ||
			chunk.ModRevision > manifest.Revision {
			clear(ciphertext)
			return Snapshot{}, errs.New(
				errs.KindInternal,
				"accepted action ciphertext chunk is missing or differs from its sealed manifest",
			)
		}
		ciphertext = append(ciphertext, chunk.Value...)
	}
	digest := sha256.Sum256(ciphertext)
	if hex.EncodeToString(digest[:]) != record.CiphertextSHA256 {
		clear(ciphertext)
		return Snapshot{}, errs.New(errs.KindInternal, "accepted action ciphertext failed content verification")
	}
	return Snapshot{Record: record, Revision: manifest.Revision, Ciphertext: ciphertext}, nil
}
