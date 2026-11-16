package acceptedactions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Stage writes inert ciphertext in measured batches before Task acceptance.
// Each chunk batch advances the descriptor CAS. The returned seal MUST join
// accepted Task/idempotency publication; no reader can execute an unsealed draft.
func (repository *Repository) Stage(ctx context.Context, record Record, ciphertext []byte,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	if len(ciphertext) == 0 || len(ciphertext) > MaximumCiphertextBytes {
		return nil, nil, errs.New(errs.KindValidationFailed, "accepted action ciphertext is too large")
	}
	digest := sha256.Sum256(ciphertext)
	if record.Validate() != nil || record.Stored != 0 || record.Sealed || uint32(len(ciphertext)) != record.Bytes ||
		hex.EncodeToString(digest[:]) != record.CiphertextSHA256 {
		return nil, nil, errs.New(
			errs.KindValidationFailed,
			"accepted action ciphertext differs from its immutable reference",
		)
	}
	taskID := record.TaskID
	offset := 0
	for _, part := range record.Parts {
		partDigest := sha256.Sum256(ciphertext[offset : offset+int(part.Bytes)])
		if hex.EncodeToString(partDigest[:]) != part.SHA256 {
			return nil, nil, errs.New(
				errs.KindValidationFailed,
				"accepted action encrypted part differs from its manifest",
			)
		}
		offset += int(part.Bytes)
	}
	value, err := encode(record)
	if err != nil {
		return nil, nil, err
	}
	defer clear(value)
	result, err := repository.commit(
		ctx,
		[]keyvalue.Condition{{Key: taskKey(taskID)}, {Key: Key(taskID)}, {Key: expiryKey(record)}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: Key(taskID), Value: value},
			{Type: keyvalue.MutationPut, Key: expiryKey(record), Value: []byte(taskID)}},
	)
	if err != nil {
		return nil, nil, err
	}
	revision := result.Revision
	for record.Stored < record.Chunks {
		conditions := []keyvalue.Condition{{Key: taskKey(taskID)}, {Key: Key(taskID), ModRevision: revision}}
		var mutations []keyvalue.Mutation
		end := min(record.Stored+8, record.Chunks)
		for index := record.Stored; index < end; index++ {
			key := chunkKey(taskID, index)
			start := int(index) * ChunkBytes
			conditions = append(conditions, keyvalue.Condition{Key: key})
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key,
				Value: ciphertext[start:min(start+ChunkBytes, len(ciphertext))]})
		}
		record.Stored = end
		manifest, err := encode(record)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: Key(taskID), Value: manifest})
		result, err = repository.commit(ctx, conditions, mutations)
		clear(manifest)
		if err != nil {
			return nil, nil, err
		}
		revision = result.Revision
	}
	record.Sealed = true
	sealed, err := encode(record)
	if err != nil {
		return nil, nil, err
	}
	kindConditions, kindMutations, err := prepareKind(record)
	if err != nil {
		clear(sealed)
		return nil, nil, err
	}
	return append(
			[]keyvalue.Condition{{Key: taskKey(taskID)}, {Key: Key(taskID), ModRevision: revision}},
			kindConditions...),
		append(
			[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: Key(taskID), Value: sealed},
				{Type: keyvalue.MutationDelete, Key: expiryKey(record)}},
			kindMutations...), nil
}
