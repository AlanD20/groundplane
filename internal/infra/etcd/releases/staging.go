package releases

import (
	"bytes"
	"context"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type VersionedReleaseManifest struct {
	Record       ReleaseStagedManifest
	Revision     int64
	ReadRevision int64
}

func (ledger *Stager) Stage(ctx context.Context, input ReleaseStage) (VersionedReleaseManifest, error) {
	if ctx == nil || ledger == nil || ledger.store == nil {
		return VersionedReleaseManifest{}, errs.New(errs.KindInternal, "release staging context or ledger is missing")
	}
	if err := ctx.Err(); err != nil {
		return VersionedReleaseManifest{}, err
	}
	input = CloneReleaseStage(input)
	if err := ValidateReleaseStage(input); err != nil {
		return VersionedReleaseManifest{}, err
	}
	refs := make([]ReleaseStagedMemberRef, len(input.Members))
	records := make([]releaseStagedWrite, 0, len(input.Members)*3)
	for index, member := range input.Members {
		intentValue, err := EncodeReleaseRecord("release-intent", member.Intent)
		if err != nil {
			clearStagedReleaseRecords(records)
			return VersionedReleaseManifest{}, err
		}
		renderValue, err := EncodeReleaseRecord("release-render-input", json.RawMessage(member.RenderInput))
		if err != nil {
			clear(intentValue)
			clearStagedReleaseRecords(records)
			return VersionedReleaseManifest{}, err
		}
		checkpointValue, err := EncodeReleaseRecord("release-checkpoint", member.Checkpoint)
		if err != nil {
			clear(intentValue)
			clear(renderValue)
			clearStagedReleaseRecords(records)
			return VersionedReleaseManifest{}, err
		}
		intentDigest, _ := domain.Digest(member.Intent)
		renderDigest, _ := domain.Digest(json.RawMessage(member.RenderInput))
		checkpointDigest, _ := domain.Digest(member.Checkpoint)
		refs[index] = ReleaseStagedMemberRef{
			ReleaseID: member.Intent.ID, ServiceID: member.Intent.ServiceID, IntentDigest: intentDigest,
			RenderDigest: renderDigest, CheckpointDigest: checkpointDigest,
		}
		records = append(records,
			releaseStagedWrite{ReleaseIntentStagingKey(input.PublicationID, member.Intent.ID), intentValue},
			releaseStagedWrite{ReleaseRenderInputStagingKey(input.PublicationID, member.Intent.ID), renderValue},
			releaseStagedWrite{ReleaseCheckpointStagingKey(input.PublicationID, member.Intent.ID), checkpointValue},
		)
	}
	defer clearStagedReleaseRecords(records)
	for start := 0; start < len(records); start += 16 {
		end := min(start+16, len(records))
		conditions := make([]etcdstore.Condition, end-start)
		mutations := make([]etcdstore.Mutation, end-start)
		for index, record := range records[start:end] {
			conditions[index] = etcdstore.Condition{Key: record.key}
			mutations[index] = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: record.key, Value: record.value}
		}
		result, err := ledger.store.Transact(ctx, conditions, mutations)
		etcdstore.ClearValues(result.FailureReads)
		if err != nil {
			return VersionedReleaseManifest{}, err
		}
		if !result.Succeeded {
			if _, _, err := ledger.matchStagedRecords(ctx, records[start:end]); err != nil {
				return VersionedReleaseManifest{}, err
			}
		}
	}
	manifest := ReleaseStagedManifest{
		PublicationID: input.PublicationID, OperationID: input.OperationID, Members: refs,
		CreatedAt: input.CreatedAt,
	}
	manifest.Digest, _ = domain.Digest(struct {
		PublicationID string                   `json:"publication_id"`
		OperationID   string                   `json:"operation_id"`
		Members       []ReleaseStagedMemberRef `json:"members"`
	}{manifest.PublicationID, manifest.OperationID, manifest.Members})
	manifestValue, err := EncodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		return VersionedReleaseManifest{}, err
	}
	defer clear(manifestValue)
	result, err := ledger.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: ReleaseManifestStagingKey(input.PublicationID)}},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: ReleaseManifestStagingKey(input.PublicationID), Value: manifestValue},
		},
	)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return VersionedReleaseManifest{}, err
	}
	if !result.Succeeded {
		revision, readRevision, err := ledger.matchStagedRecords(ctx, []releaseStagedWrite{
			{ReleaseManifestStagingKey(input.PublicationID), manifestValue},
		})
		if err != nil {
			return VersionedReleaseManifest{}, err
		}
		return VersionedReleaseManifest{Record: manifest, Revision: revision, ReadRevision: readRevision}, nil
	}
	return VersionedReleaseManifest{Record: manifest, Revision: result.Revision, ReadRevision: result.Revision}, nil
}

// A producer may restart after a staging write committed. Resume only exact
// immutable bytes, without overwriting a draft or adopting another operation.
func (ledger *Stager) matchStagedRecords(ctx context.Context, records []releaseStagedWrite) (int64, int64, error) {
	keys := make([]string, len(records))
	for index, record := range records {
		keys[index] = record.key
	}
	read, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return 0, 0, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(records) || len(records) == 0 {
		return 0, 0, errs.New(errs.KindStateConflict, "release staging replay evidence is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != records[index].key || value.ModRevision <= 0 ||
			value.ModRevision > read.ReadRevision || !bytes.Equal(value.Value, records[index].value) {
			return 0, 0, errs.New(errs.KindStateConflict, "release staging identity differs from captured input")
		}
	}
	return read.Values[0].ModRevision, read.ReadRevision, nil
}

func clearStagedReleaseRecords(records []releaseStagedWrite) {
	for index := range records {
		clear(records[index].value)
	}
}
