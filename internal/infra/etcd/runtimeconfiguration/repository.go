package runtimeconfiguration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	stagingPrefix = "/v1/staging/runtime-configuration-sources/"
	recordsPrefix = "/v1/records/runtime-configuration-sources/"
	readBatchSize = 64
)

type Repository struct{ store Store }

func newRepository(store Store) (*Repository, error) {
	if store == nil {
		return nil, errs.New(errs.KindValidationFailed, "runtime configuration store is missing")
	}
	return &Repository{store: store}, nil
}

// Stage reserves one immutable manifest, stages each member independently,
// and publishes the root last. It does not publish applied-state authority.
func (repository *Repository) Stage(ctx context.Context, snapshot Snapshot) (Reference, error) {
	if ctx == nil {
		return Reference{}, errs.New(errs.KindValidationFailed, "runtime configuration stage context is missing")
	}
	canonical, err := canonicalizeSnapshot(snapshot)
	if err != nil {
		return Reference{}, err
	}
	stagingRevision, sealed, err := repository.ensureManifest(ctx, canonical)
	if err != nil {
		return Reference{}, err
	}
	if sealed {
		return canonical.reference, nil
	}
	for _, member := range canonical.members {
		sealed, err = repository.stageMember(ctx, canonical, stagingRevision, member)
		if err != nil {
			return Reference{}, err
		}
		if sealed {
			return canonical.reference, nil
		}
	}
	return repository.seal(ctx, canonical, stagingRevision)
}

// LoadRetained resolves the exact immutable reference at a fresh fixed storage
// revision. It survives compaction of the original acknowledgement revision;
// it does not read a mutable desired or applied head.
func (repository *Repository) LoadRetained(ctx context.Context, reference Reference) (Snapshot, error) {
	if ctx == nil || ValidateReference(reference) != nil {
		return Snapshot{}, errs.New(errs.KindValidationFailed, "retained configuration authority is invalid")
	}
	read, err := repository.read(ctx, []string{publishedRootKey(reference.ID)}, 0)
	if err != nil {
		return Snapshot{}, err
	}
	return repository.Load(ctx, reference, read.ReadRevision)
}

// Load resolves a published immutable snapshot at the caller's fixed owning
// revision. No latest-state fallback is permitted.
func (repository *Repository) Load(
	ctx context.Context,
	reference Reference,
	fixedRevision int64,
) (Snapshot, error) {
	if ctx == nil || fixedRevision <= 0 {
		return Snapshot{}, errs.New(errs.KindValidationFailed, "runtime configuration load authority is invalid")
	}
	if err := ValidateReference(reference); err != nil {
		return Snapshot{}, err
	}
	rootKey := publishedRootKey(reference.ID)
	read, err := repository.read(ctx, []string{rootKey}, fixedRevision)
	if err != nil {
		return Snapshot{}, err
	}
	if read.Values[0] == nil {
		return Snapshot{}, corrupt("runtime configuration root is missing")
	}
	manifest, err := decodeManifest(read.Values[0].Value)
	if err != nil {
		return Snapshot{}, err
	}
	if digest(read.Values[0].Value) != reference.SHA256 || manifest.ID != reference.ID ||
		manifest.EnvironmentID != reference.EnvironmentID || manifest.Generation != reference.Generation {
		return Snapshot{}, corrupt("runtime configuration root does not match its reference")
	}
	files := make([]taskmaterialization.Record, 0, len(manifest.Members))
	for start := 0; start < len(manifest.Members); start += readBatchSize {
		end := min(start+readBatchSize, len(manifest.Members))
		keys := make([]string, end-start)
		for index, member := range manifest.Members[start:end] {
			keys[index] = memberKey(reference.ID, member)
		}
		members, readErr := repository.read(ctx, keys, fixedRevision)
		if readErr != nil {
			return Snapshot{}, readErr
		}
		for index, value := range members.Values {
			manifestMember := manifest.Members[start+index]
			if value == nil {
				return Snapshot{}, corrupt("runtime configuration member is missing")
			}
			if digest(value.Value) != manifestMember.RecordSHA256 {
				return Snapshot{}, corrupt("runtime configuration member digest does not match the manifest")
			}
			stored, decodeErr := decodeMember(value.Value)
			if decodeErr != nil {
				return Snapshot{}, decodeErr
			}
			if stored.SnapshotID != reference.ID || stored.EnvironmentID != reference.EnvironmentID ||
				stored.Generation != reference.Generation || stored.Record.Destination != manifestMember.Destination ||
				stored.Record.SHA256 != manifestMember.ContentSHA256 {
				return Snapshot{}, corrupt("runtime configuration member does not match the manifest")
			}
			files = append(files, stored.Record)
		}
	}
	return Snapshot{
		ID: reference.ID, EnvironmentID: reference.EnvironmentID,
		Generation: reference.Generation, Files: taskmaterialization.Clone(files),
	}, nil
}

func (repository *Repository) ensureManifest(
	ctx context.Context,
	canonical canonicalSnapshot,
) (int64, bool, error) {
	keys := []string{publishedRootKey(canonical.snapshot.ID), stagingRootKey(canonical.snapshot.ID)}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return 0, false, err
	}
	stagingRevision, sealed, err := repository.inspectManifest(ctx, canonical, read)
	if err != nil || sealed || stagingRevision > 0 {
		return stagingRevision, sealed, err
	}
	result, err := repository.transact(ctx,
		[]Condition{{Key: keys[0]}, {Key: keys[1]}},
		[]Mutation{{Type: MutationPut, Key: keys[1], Value: canonical.rootValue}},
	)
	if err != nil {
		return 0, false, err
	}
	if result.Succeeded {
		if result.Revision <= 0 {
			return 0, false, corrupt("runtime configuration staging revision is invalid")
		}
		return result.Revision, false, nil
	}
	retry, err := repository.read(ctx, keys, 0)
	if err != nil {
		return 0, false, err
	}
	if retry.Values[0] != nil && retry.Values[1] == nil {
		return repository.inspectManifest(ctx, canonical, retry)
	}
	stagingRevision, sealed, err = repository.inspectManifest(ctx, canonical, retry)
	if err == nil && !sealed && stagingRevision == 0 {
		err = errs.New(errs.KindStateConflict, "runtime configuration identity raced durable state")
	}
	return stagingRevision, sealed, err
}

func (repository *Repository) inspectManifest(
	ctx context.Context,
	canonical canonicalSnapshot,
	read *GetManyResult,
) (int64, bool, error) {
	published, staging := read.Values[0], read.Values[1]
	if published != nil {
		if staging != nil {
			return 0, false, corrupt("published runtime configuration retained a staging root")
		}
		return 0, true, repository.verifyPublished(ctx, canonical, published, read.ReadRevision)
	}
	if staging == nil {
		return 0, false, nil
	}
	if _, err := decodeManifest(staging.Value); err != nil {
		return 0, false, err
	}
	if !bytes.Equal(staging.Value, canonical.rootValue) {
		return 0, false, errs.New(errs.KindStateConflict, "runtime configuration identity has different bytes")
	}
	return staging.ModRevision, false, nil
}

func (repository *Repository) stageMember(
	ctx context.Context,
	canonical canonicalSnapshot,
	stagingRevision int64,
	member canonicalMember,
) (bool, error) {
	keys := []string{
		publishedRootKey(canonical.snapshot.ID), stagingRootKey(canonical.snapshot.ID), member.key,
	}
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return false, err
	}
	present, sealed, err := repository.inspectMember(ctx, canonical, stagingRevision, member, read)
	if err != nil || present || sealed {
		return sealed, err
	}
	result, err := repository.transact(ctx,
		[]Condition{{Key: keys[0]}, {Key: keys[1], ModRevision: stagingRevision}, {Key: keys[2]}},
		[]Mutation{{Type: MutationPut, Key: keys[2], Value: member.value}},
	)
	if err != nil {
		return false, err
	}
	if result.Succeeded {
		if result.Revision <= 0 {
			return false, corrupt("runtime configuration member staging revision is invalid")
		}
		return false, nil
	}
	retry, err := repository.read(ctx, keys, 0)
	if err != nil {
		return false, err
	}
	present, sealed, err = repository.inspectMember(ctx, canonical, stagingRevision, member, retry)
	if err != nil || present || sealed {
		return sealed, err
	}
	return false, errs.New(errs.KindStateConflict, "runtime configuration member staging raced durable state")
}

func (repository *Repository) inspectMember(
	ctx context.Context,
	canonical canonicalSnapshot,
	stagingRevision int64,
	member canonicalMember,
	read *GetManyResult,
) (bool, bool, error) {
	published, staging, value := read.Values[0], read.Values[1], read.Values[2]
	if published != nil {
		if staging != nil {
			return false, false, corrupt("published runtime configuration retained a staging root")
		}
		return false, true, repository.verifyPublished(ctx, canonical, published, read.ReadRevision)
	}
	if staging == nil || staging.ModRevision != stagingRevision || !bytes.Equal(staging.Value, canonical.rootValue) {
		return false, false, errs.New(errs.KindStateConflict, "runtime configuration staging authority changed")
	}
	if value == nil {
		return false, false, nil
	}
	if _, err := decodeMember(value.Value); err != nil {
		return false, false, err
	}
	if !bytes.Equal(value.Value, member.value) {
		return false, false, errs.New(
			errs.KindStateConflict,
			"runtime configuration member identity has different bytes",
		)
	}
	return true, false, nil
}

func (repository *Repository) seal(
	ctx context.Context,
	canonical canonicalSnapshot,
	stagingRevision int64,
) (Reference, error) {
	publishedKey := publishedRootKey(canonical.snapshot.ID)
	stagingKey := stagingRootKey(canonical.snapshot.ID)
	result, err := repository.transact(ctx,
		[]Condition{{Key: publishedKey}, {Key: stagingKey, ModRevision: stagingRevision}},
		[]Mutation{
			{Type: MutationPut, Key: publishedKey, Value: canonical.rootValue},
			{Type: MutationDelete, Key: stagingKey},
		},
	)
	if err != nil {
		return Reference{}, err
	}
	if result.Succeeded {
		if result.Revision <= 0 {
			return Reference{}, corrupt("runtime configuration publication revision is invalid")
		}
		return canonical.reference, nil
	}
	read, err := repository.read(ctx, []string{publishedKey, stagingKey}, 0)
	if err != nil {
		return Reference{}, err
	}
	if read.Values[0] != nil && read.Values[1] == nil {
		if err := repository.verifyPublished(ctx, canonical, read.Values[0], read.ReadRevision); err != nil {
			return Reference{}, err
		}
		return canonical.reference, nil
	}
	return Reference{}, errs.New(errs.KindStateConflict, "runtime configuration publication raced durable state")
}

func (repository *Repository) verifyPublished(
	ctx context.Context,
	canonical canonicalSnapshot,
	value *KeyValue,
	readRevision int64,
) error {
	if _, err := decodeManifest(value.Value); err != nil {
		return err
	}
	if !bytes.Equal(value.Value, canonical.rootValue) {
		return errs.New(
			errs.KindStateConflict,
			"runtime configuration identity is already published with different bytes",
		)
	}
	_, err := repository.Load(ctx, canonical.reference, readRevision)
	return err
}

func (repository *Repository) read(
	ctx context.Context,
	keys []string,
	revision int64,
) (*GetManyResult, error) {
	result, err := repository.store.GetMany(ctx, keys, revision)
	if err != nil {
		return nil, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if result == nil || len(result.Values) != len(keys) || result.ReadRevision < 0 ||
		(revision > 0 && result.ReadRevision != revision) {
		return nil, corrupt("runtime configuration store read is inconsistent")
	}
	for index, value := range result.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 ||
			value.ModRevision > result.ReadRevision) {
			return nil, corrupt("runtime configuration store value is inconsistent")
		}
	}
	return result, nil
}

func (repository *Repository) transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TxnResult, error) {
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return TxnResult{}, errs.Wrap(errs.KindStorageUnavailable, err)
	}
	return result, nil
}

func stagingRootKey(id string) string   { return stagingPrefix + id + "/root" }
func publishedRootKey(id string) string { return recordsPrefix + id + "/root" }

func memberKey(id string, member manifestMember) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(member.Destination))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(member.ContentSHA256))
	return recordsPrefix + id + "/members/" + hex.EncodeToString(hasher.Sum(nil))
}
