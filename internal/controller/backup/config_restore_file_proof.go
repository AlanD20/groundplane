package backup

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// ProveCandidateFiles uses only the original staged generations/receipts at one
// MVCC view. It computes the expected host proof before live publication; it
// never resolves a SecretRef or Attach fact against today's state.
func (producer *ConfigRestoreProducer) ProveCandidateFiles(
	ctx context.Context,
	candidate *ConfigRestorePublication,
) error {
	if ctx == nil || producer == nil || candidate == nil || candidate.filePlan == nil {
		return configSnapshotInvalid()
	}
	owner := candidate.SourceSeal.Record.Owner
	read, err := producer.sources.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(owner.Transfer.Binding.TaskID)}},
	)
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 || read.Values[0] == nil {
		return configSnapshotGuardConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	readValue := func(ctx context.Context, index int) (backupconfig.Entry, []byte, error) {
		if index < 0 || index >= len(candidate.fileEntries) || index >= len(candidate.Projection.Entries) {
			return backupconfig.Entry{}, nil, configSnapshotGuardConflict()
		}
		record := candidate.Projection.Entries[index]
		if record.Entry.ID != candidate.fileEntries[index].ID {
			return backupconfig.Entry{}, nil, configSnapshotGuardConflict()
		}
		if _, err := backupconfiguration.PrepareConfigRestoreEntryPublication(ctx, producer.sources, owner, uint32(index+1), record, read.ReadRevision); err != nil {
			return backupconfig.Entry{}, nil, err
		}
		key := entryvalues.PlainKey(record.Entry.ID, owner.GenerationID)
		if record.Entry.Secret {
			key = entryvalues.SecretKey(record.Entry.ID, owner.GenerationID)
		}
		values, err := producer.sources.GetMany(
			ctx,
			etcdstore.GetManyRequest{Keys: []string{key}, Revision: read.ReadRevision},
		)
		if err != nil {
			return backupconfig.Entry{}, nil, err
		}
		if values == nil || values.ReadRevision != read.ReadRevision || len(values.Values) != 1 ||
			values.Values[0] == nil {
			return backupconfig.Entry{}, nil, configSnapshotGuardConflict()
		}
		defer etcdstore.ClearValues(values.Values)
		var generation entries.EntryValueGeneration
		if record.Entry.Secret {
			decoded, err := entryvalues.DecodeSecret(values.Values[0].Value)
			if err != nil {
				return backupconfig.Entry{}, nil, err
			}
			generation.Secret = &decoded
		} else {
			decoded, err := entryvalues.DecodePlain(values.Values[0].Value)
			if err != nil {
				return backupconfig.Entry{}, nil, err
			}
			generation.Plain = &decoded
		}
		defer clearRestoredEntryValue(generation)
		var content []byte
		err = openRestoredEntryValue(
			ctx,
			producer.protector,
			generation,
			candidate.createdAt,
			func(value []byte) error {
				content = append([]byte(nil), value...)
				return nil
			},
		)
		if err != nil {
			clear(content)
			return backupconfig.Entry{}, nil, err
		}
		return candidate.fileEntries[index], content, nil
	}
	headers, err := candidate.filePlan.PrepareHeaders(
		ctx,
		owner.Transfer.Binding.TaskID,
		candidate.fileStepID,
		readValue,
	)
	if err != nil {
		return err
	}
	candidate.FileProof, err = candidate.filePlan.Proof(headers)
	return err
}
