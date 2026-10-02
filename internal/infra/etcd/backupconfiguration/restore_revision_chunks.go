package backupconfiguration

import (
	"bytes"
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *ConfigTransferRepository) stageRestoreRevisionChunks(ctx context.Context,
	generation etcdstore.Versioned[ConfigRestoreGenerationRecord], rootKey string,
	chunks []etcdstore.Mutation,
) error {
	read, conditions, err := repository.restoreRevisionAuthority(ctx, generation, rootKey)
	if err != nil {
		return err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[3] != nil {
		// Once sealed, missing chunks are corruption, never restart authority.
		return captureSnapshotConflict()
	}
	keys := restoreRevisionChunkKeys(chunks)
	page, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: read.ReadRevision})
	if err != nil {
		return err
	}
	if page == nil || page.ReadRevision != read.ReadRevision || len(page.Values) != len(keys) {
		return captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(page.Values)
	var missing []etcdstore.Mutation
	for index, chunk := range chunks {
		condition := etcdstore.Condition{Key: chunk.Key}
		if value := page.Values[index]; value != nil {
			if value.Key != chunk.Key || value.Version != 1 || value.ModRevision <= 0 ||
				!bytes.Equal(value.Value, chunk.Value) {
				return captureSnapshotConflict()
			}
			condition.ModRevision = value.ModRevision
		} else {
			missing = append(missing, chunk)
		}
		conditions = append(conditions, condition)
	}
	if len(missing) == 0 {
		return nil
	}
	_, err = repository.commitRestoreRevision(ctx, conditions, missing)
	return err
}

func (repository *ConfigTransferRepository) verifyRestoreRevisionChunks(ctx context.Context,
	chunks []etcdstore.Mutation, revision, sealRevision int64,
) ([]etcdstore.Condition, error) {
	keys := restoreRevisionChunkKeys(chunks)
	page, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if page == nil || page.ReadRevision != revision || len(page.Values) != len(keys) {
		return nil, captureSnapshotConflict()
	}
	defer etcdstore.ClearValues(page.Values)
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for index, chunk := range chunks {
		value := page.Values[index]
		if value == nil || value.Key != chunk.Key || value.Version != 1 || value.ModRevision <= 0 ||
			(sealRevision > 0 && value.ModRevision >= sealRevision) ||
			!bytes.Equal(value.Value, chunk.Value) {
			return nil, captureSnapshotConflict()
		}
		conditions = append(conditions, etcdstore.Condition{Key: chunk.Key, ModRevision: value.ModRevision})
	}
	return conditions, nil
}

func restoreRevisionChunkKeys(chunks []etcdstore.Mutation) []string {
	keys := make([]string, len(chunks))
	for index, chunk := range chunks {
		keys[index] = chunk.Key
	}
	return keys
}
