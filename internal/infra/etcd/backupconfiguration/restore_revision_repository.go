package backupconfiguration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// StageConfigRestoreRevision makes the complete prepared revision durable
// before the final transfer grant. It never switches the desired head or
// changes primary Entries. Interrupted staging resumes only identical chunks.
func (repository *ConfigTransferRepository) StageConfigRestoreRevision(ctx context.Context,
	generation etcdstore.Versioned[ConfigRestoreGenerationRecord], pointID string,
	revision blueprints.ConfigRestoreRevision,
) (int64, error) {
	owner := generation.Record.Owner
	if generation.Record.Completed.GetContent() == nil {
		return 0, captureSnapshotConflict()
	}
	digest, err := hex.DecodeString(owner.SourceSHA256)
	if err != nil || len(digest) != sha256.Size {
		return 0, captureSnapshotConflict()
	}
	audit := blueprints.EnvironmentConfigRestoreAudit{BaseRevisionID: owner.BaselineRevisionID,
		PointID: pointID, GenerationID: owner.GenerationID}
	copy(audit.SourceSHA256[:], digest)
	if err := revision.Validate(owner.EnvironmentID, owner.Transfer.Binding.TaskID,
		owner.RenderGeneration, owner.BaselineHeadRevision, generation.Record.Completed.Content.EntryCount, audit); err != nil {
		return 0, err
	}
	root, err := blueprints.EncodeEnvironmentBlueprintSeal(revision.Seal)
	if err != nil {
		return 0, err
	}
	defer clear(root)
	chunks, err := restoreRevisionChunks(revision)
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearMutationValues(chunks)
	rootKey := blueprints.EnvironmentBlueprintRootKey(owner.EnvironmentID, owner.Transfer.Binding.TaskID)
	read, _, err := repository.restoreRevisionAuthority(ctx, generation, rootKey)
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearValues(read.Values)
	if existing := read.Values[3]; existing != nil {
		if existing.Key != rootKey || existing.Version != 1 || existing.ModRevision <= 0 ||
			!bytes.Equal(existing.Value, root) {
			return 0, captureSnapshotConflict()
		}
		if _, err := repository.verifyRestoreRevisionChunks(ctx, chunks, read.ReadRevision, existing.ModRevision); err != nil {
			return 0, err
		}
		return existing.ModRevision, nil
	}
	for offset := 0; offset < len(chunks); offset += blueprints.EnvironmentBlueprintStageBatchChunks {
		end := min(offset+blueprints.EnvironmentBlueprintStageBatchChunks, len(chunks))
		if err := repository.stageRestoreRevisionChunks(ctx, generation, rootKey, chunks[offset:end]); err != nil {
			return 0, err
		}
	}
	// Re-read native authority at the same view used to verify all completed
	// chunks. A racing timeout, abort, publication or head change defeats CAS.
	etcdstore.ClearValues(read.Values)
	read, conditions, err := repository.restoreRevisionAuthority(ctx, generation, rootKey)
	if err != nil {
		return 0, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[3] != nil {
		if read.Values[3].Version != 1 || !bytes.Equal(read.Values[3].Value, root) {
			return 0, captureSnapshotConflict()
		}
		if _, err := repository.verifyRestoreRevisionChunks(ctx, chunks, read.ReadRevision, read.Values[3].ModRevision); err != nil {
			return 0, err
		}
		return read.Values[3].ModRevision, nil
	}
	chunkConditions, err := repository.verifyRestoreRevisionChunks(ctx, chunks, read.ReadRevision, 0)
	if err != nil {
		return 0, err
	}
	conditions = append(conditions, chunkConditions...)
	result, err := repository.commitRestoreRevision(ctx, conditions,
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: rootKey, Value: root}})
	return result, err
}

func restoreRevisionChunks(revision blueprints.ConfigRestoreRevision) ([]etcdstore.Mutation, error) {
	var result []etcdstore.Mutation
	for _, stream := range []struct {
		family uint8
		data   []byte
	}{
		{blueprints.EnvironmentBlueprintChunkAudit, revision.Audit},
		{blueprints.EnvironmentBlueprintChunkProjection, revision.DesiredInput},
	} {
		for offset, sequence := 0, uint32(0); offset < len(stream.data); sequence++ {
			end := min(offset+blueprints.EnvironmentBlueprintChunkBytes, len(stream.data))
			data := stream.data[offset:end]
			encoded, err := blueprints.EncodeEnvironmentBlueprintChunk(blueprints.EnvironmentBlueprintChunk{
				Family: stream.family, Sequence: sequence, LogicalOffset: uint64(offset),
				LogicalLength: uint32(len(data)), Digest: sha256.Sum256(data), Data: data})
			if err != nil {
				etcdstore.ClearMutationValues(result)
				return nil, err
			}
			result = append(result, etcdstore.Mutation{Type: etcdstore.MutationPut,
				Key: blueprints.EnvironmentBlueprintChunkKeyFor(revision.Seal.EnvironmentID,
					revision.Seal.RevisionID, stream.family, sequence), Value: encoded})
			offset = end
		}
	}
	return result, nil
}

func (repository *ConfigTransferRepository) restoreRevisionAuthority(ctx context.Context,
	generation etcdstore.Versioned[ConfigRestoreGenerationRecord], rootKey string,
) (*etcdstore.GetManyResult, []etcdstore.Condition, error) {
	owner := generation.Record.Owner
	if ctx == nil || repository == nil || repository.guard == nil || repository.restoreGuard == nil ||
		generation.Revision <= 0 || ValidateConfigRestoreTransferOwner(owner) != nil {
		return nil, nil, captureSnapshotConflict()
	}
	expected, err := EncodeConfigRestoreGeneration(generation.Record)
	if err != nil {
		return nil, nil, err
	}
	defer clear(expected)
	keys := []string{ConfigRestoreGenerationKey(owner), ConfigRestoreEntryCursorKey(owner),
		blueprints.EnvironmentBlueprintHeadKey(owner.EnvironmentID), rootKey}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return nil, nil, captureSnapshotConflict()
	}
	fail := func(err error) (*etcdstore.GetManyResult, []etcdstore.Condition, error) {
		etcdstore.ClearValues(read.Values)
		return nil, nil, err
	}
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0) {
			return fail(captureSnapshotConflict())
		}
	}
	if read.Values[0] == nil || read.Values[0].Version != 1 || read.Values[0].ModRevision != generation.Revision ||
		!bytes.Equal(
			read.Values[0].Value,
			expected,
		) || read.Values[2] == nil || read.Values[2].ModRevision != owner.BaselineHeadRevision {
		return fail(captureSnapshotConflict())
	}
	head, err := idempotency.DecodeTaskReference(read.Values[2].Value)
	if err != nil || head != owner.BaselineRevisionID {
		return fail(captureSnapshotConflict())
	}
	count := generation.Record.Completed.Content.EntryCount
	if cursor := read.Values[1]; cursor != nil {
		decoded, err := decodeConfigRestoreEntryCursor(cursor.Value)
		if err != nil || decoded.Owner != owner || decoded.LastOrdinal != count {
			return fail(captureSnapshotConflict())
		}
	} else if count != 0 {
		return fail(captureSnapshotConflict())
	}
	conditions, err := repository.guard(ctx, owner.Transfer, read.ReadRevision)
	if err != nil {
		return fail(err)
	}
	more, err := repository.restoreGuard(ctx, owner, read.ReadRevision)
	if err != nil {
		return fail(err)
	}
	conditions = append(conditions, more...)
	for index, key := range keys {
		condition := etcdstore.Condition{Key: key}
		if value := read.Values[index]; value != nil {
			condition.ModRevision = value.ModRevision
		}
		conditions = append(conditions, condition)
	}
	return read, conditions, nil
}

func (repository *ConfigTransferRepository) commitRestoreRevision(ctx context.Context,
	conditions []etcdstore.Condition, mutations []etcdstore.Mutation,
) (int64, error) {
	budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil {
		return 0, err
	}
	if budget.Operations > etcdstore.MaximumOperations || budget.Bytes > 768<<10 {
		return 0, captureSnapshotConflict()
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded || result.Revision <= 0 {
		return 0, captureSnapshotConflict()
	}
	return result.Revision, nil
}
