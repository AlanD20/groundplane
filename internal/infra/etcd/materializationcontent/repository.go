// Package materializationcontent retains exact non-secret generated Component
// file bytes independently of the Task and renderer that produced them.
package materializationcontent

import (
	"bytes"
	"context"
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	stagingPrefix = "/v1/staging/component-materialization-content/"
	recordsPrefix = "/v1/records/component-materialization-content/"
)

type Repository struct{ store Store }

func newRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "component materialization content store is missing")
	}
	return &Repository{store: store}, nil
}

// Stage reserves the exact identity, writes immutable chunks, and publishes
// the loadable root last. It accepts no secret-derived materialization kind.
func (repository *Repository) Stage(
	ctx context.Context,
	record taskmaterialization.Record,
	generation uint64,
	content []byte,
) error {
	if ctx == nil {
		return errs.New(errs.KindValidationFailed, "component materialization stage context is missing")
	}
	canonical, err := canonicalize(record, generation, content)
	if err != nil {
		return err
	}
	stagingRevision, sealed, err := repository.claim(ctx, canonical)
	if err != nil || sealed {
		return err
	}
	for ordinal, value := range canonical.chunkValues {
		sealed, err = repository.stageChunk(ctx, canonical, stagingRevision, ordinal, value)
		if err != nil || sealed {
			return err
		}
	}
	return repository.seal(ctx, canonical, stagingRevision)
}

// Load returns the exact published bytes named by the complete durable
// materialization record. Missing content never falls back to a renderer.
// generation is the consuming Task generation: unchanged retained files may
// originate earlier, but a future generation cannot supply this execution.
func (repository *Repository) Load(
	ctx context.Context,
	record taskmaterialization.Record,
	generation uint64,
) ([]byte, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindValidationFailed, "component materialization load context is missing")
	}
	if err := validateComponentRecord(record, generation); err != nil {
		return nil, err
	}
	rootKey := publishedRootKey(record.EnvironmentID, record.MaterializationID)
	root, err := repository.read(ctx, []string{rootKey}, 0)
	if err != nil {
		return nil, err
	}
	if root.Values[0] == nil || root.ReadRevision <= 0 {
		return nil, corrupt("component materialization content root is missing")
	}
	manifest, err := decodeManifest(root.Values[0].Value)
	if err != nil {
		return nil, err
	}
	if manifest.Generation > generation || !sameRecord(manifest.Record, record) {
		return nil, corrupt("component materialization content root does not match its reference")
	}
	keys := make([]string, len(manifest.Chunks))
	for ordinal := range keys {
		keys[ordinal] = chunkKey(record.EnvironmentID, record.MaterializationID, ordinal)
	}
	chunks := &GetManyResult{Values: []*KeyValue{}, ReadRevision: root.ReadRevision}
	if len(keys) > 0 {
		chunks, err = repository.read(ctx, keys, root.ReadRevision)
		if err != nil {
			return nil, err
		}
	}
	content := make([]byte, 0, record.Length)
	for ordinal, value := range chunks.Values {
		if value == nil || digest(value.Value) != manifest.Chunks[ordinal] {
			clear(content)
			return nil, corrupt("component materialization content chunk is missing or mismatched")
		}
		chunk, decodeErr := decodeChunk(value.Value)
		if decodeErr != nil {
			clear(content)
			return nil, decodeErr
		}
		if chunk.EnvironmentID != record.EnvironmentID || chunk.MaterializationID != record.MaterializationID ||
			chunk.Ordinal != uint32(ordinal) || chunk.Offset != uint64(len(content)) {
			clear(chunk.Content)
			clear(content)
			return nil, corrupt("component materialization content chunk identity is inconsistent")
		}
		content = append(content, chunk.Content...)
		clear(chunk.Content)
	}
	if uint64(len(content)) != record.Length || digest(content) != record.SHA256 {
		clear(content)
		return nil, corrupt("component materialization reconstructed content is inconsistent")
	}
	return content, nil
}

func (repository *Repository) claim(
	ctx context.Context,
	canonical canonicalContent,
) (int64, bool, error) {
	keys := []string{canonical.publishedRoot, canonical.stagingRoot}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return 0, false, err
	}
	if read.Values[0] != nil && read.Values[1] != nil {
		return 0, false, corrupt("component materialization has both staging and published roots")
	}
	if read.Values[0] != nil {
		return 0, true, repository.matchPublished(ctx, canonical, read.Values[0], read.ReadRevision)
	}
	if read.Values[1] != nil {
		return repository.matchStaging(canonical, read.Values[1])
	}
	result, err := repository.transact(ctx,
		[]Condition{{Key: keys[0]}, {Key: keys[1]}},
		[]Mutation{{Type: MutationPut, Key: keys[1], Value: canonical.manifestValue}},
	)
	if err != nil {
		return 0, false, err
	}
	if result.Succeeded && result.Revision > 0 {
		return result.Revision, false, nil
	}
	retry, err := repository.read(ctx, keys, 0)
	if err != nil {
		return 0, false, err
	}
	if retry.Values[0] != nil {
		if retry.Values[1] != nil {
			return 0, false, corrupt("component materialization has both staging and published roots")
		}
		return 0, true, repository.matchPublished(ctx, canonical, retry.Values[0], retry.ReadRevision)
	}
	if retry.Values[1] != nil {
		return repository.matchStaging(canonical, retry.Values[1])
	}
	return 0, false, errs.New(errs.KindStateConflict, "component materialization content claim raced durable state")
}

func (repository *Repository) matchStaging(
	canonical canonicalContent,
	value *KeyValue,
) (int64, bool, error) {
	if _, err := decodeManifest(value.Value); err != nil {
		return 0, false, err
	}
	if !bytes.Equal(value.Value, canonical.manifestValue) {
		return 0, false, errs.New(errs.KindStateConflict, "component materialization id has different content")
	}
	return value.ModRevision, false, nil
}

func (repository *Repository) stageChunk(
	ctx context.Context,
	canonical canonicalContent,
	stagingRevision int64,
	ordinal int,
	value []byte,
) (bool, error) {
	key := chunkKey(
		canonical.materialization.EnvironmentID, canonical.materialization.MaterializationID, ordinal,
	)
	read, err := repository.read(ctx, []string{canonical.publishedRoot, canonical.stagingRoot, key}, 0)
	if err != nil {
		return false, err
	}
	if read.Values[0] != nil {
		if read.Values[1] != nil {
			return false, corrupt("component materialization has both staging and published roots")
		}
		return true, repository.matchPublished(ctx, canonical, read.Values[0], read.ReadRevision)
	}
	if read.Values[1] == nil || read.Values[1].ModRevision != stagingRevision ||
		!bytes.Equal(read.Values[1].Value, canonical.manifestValue) {
		return false, errs.New(errs.KindStateConflict, "component materialization staging authority changed")
	}
	if read.Values[2] != nil {
		if !bytes.Equal(read.Values[2].Value, value) {
			return false, errs.New(errs.KindStateConflict, "component materialization chunk has different bytes")
		}
		return false, nil
	}
	result, err := repository.transact(ctx,
		[]Condition{
			{Key: canonical.publishedRoot},
			{Key: canonical.stagingRoot, ModRevision: stagingRevision},
			{Key: key},
		},
		[]Mutation{{Type: MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return false, err
	}
	if result.Succeeded && result.Revision > 0 {
		return false, nil
	}
	retry, retryErr := repository.read(ctx, []string{canonical.publishedRoot, canonical.stagingRoot, key}, 0)
	if retryErr != nil {
		return false, retryErr
	}
	if retry.Values[0] != nil {
		if retry.Values[1] != nil {
			return false, corrupt("component materialization has both staging and published roots")
		}
		return true, repository.matchPublished(ctx, canonical, retry.Values[0], retry.ReadRevision)
	}
	if retry.Values[1] != nil && retry.Values[1].ModRevision == stagingRevision &&
		bytes.Equal(retry.Values[1].Value, canonical.manifestValue) && retry.Values[2] != nil &&
		bytes.Equal(retry.Values[2].Value, value) {
		return false, nil
	}
	return false, errs.New(errs.KindStateConflict, "component materialization chunk staging raced durable state")
}

func (repository *Repository) seal(
	ctx context.Context,
	canonical canonicalContent,
	stagingRevision int64,
) error {
	result, err := repository.transact(ctx,
		[]Condition{
			{Key: canonical.publishedRoot},
			{Key: canonical.stagingRoot, ModRevision: stagingRevision},
		},
		[]Mutation{
			{Type: MutationPut, Key: canonical.publishedRoot, Value: canonical.manifestValue},
			{Type: MutationDelete, Key: canonical.stagingRoot},
		},
	)
	if err != nil {
		return err
	}
	if result.Succeeded && result.Revision > 0 {
		return nil
	}
	read, err := repository.read(ctx, []string{canonical.publishedRoot}, 0)
	if err != nil {
		return err
	}
	if read.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "component materialization publication raced durable state")
	}
	return repository.matchPublished(ctx, canonical, read.Values[0], read.ReadRevision)
}

func (repository *Repository) matchPublished(
	ctx context.Context,
	canonical canonicalContent,
	value *KeyValue,
	revision int64,
) error {
	if _, err := decodeManifest(value.Value); err != nil {
		return err
	}
	if !bytes.Equal(value.Value, canonical.manifestValue) {
		return errs.New(errs.KindStateConflict, "component materialization id is already published differently")
	}
	content, err := repository.Load(ctx, canonical.materialization, canonical.manifest.Generation)
	clear(content)
	return err
}

func (repository *Repository) read(ctx context.Context, keys []string, revision int64) (*GetManyResult, error) {
	result, err := repository.store.GetMany(ctx, keys, revision)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if result == nil || len(result.Values) != len(keys) || result.ReadRevision < 0 ||
		(revision > 0 && result.ReadRevision != revision) {
		return nil, corrupt("component materialization content read is inconsistent")
	}
	for index, value := range result.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 ||
			value.ModRevision > result.ReadRevision) {
			return nil, corrupt("component materialization content value is inconsistent")
		}
	}
	return result, nil
}

func (repository *Repository) transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return TransactionResult{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return result, nil
}

func sameRecord(left, right taskmaterialization.Record) bool {
	if left.StepID != right.StepID || left.MaterializationID != right.MaterializationID ||
		left.EnvironmentID != right.EnvironmentID || left.Destination != right.Destination ||
		left.ServiceID != right.ServiceID || left.ServiceName != right.ServiceName ||
		left.OutputKind != right.OutputKind || left.UID != right.UID || left.GID != right.GID ||
		left.Mode != right.Mode || left.Length != right.Length || left.SHA256 != right.SHA256 ||
		left.Source.Kind != right.Source.Kind || left.Source.ComponentFile == nil || right.Source.ComponentFile == nil {
		return false
	}
	return *left.Source.ComponentFile == *right.Source.ComponentFile
}

func stagingRootKey(environmentID, materializationID string) string {
	return stagingPrefix + environmentID + "/" + materializationID + "/root"
}

func publishedRootKey(environmentID, materializationID string) string {
	return recordsPrefix + environmentID + "/" + materializationID + "/root"
}

func chunkKey(environmentID, materializationID string, ordinal int) string {
	return recordsPrefix + environmentID + "/" + materializationID + "/chunks/" + fmt.Sprintf("%02d", ordinal)
}
