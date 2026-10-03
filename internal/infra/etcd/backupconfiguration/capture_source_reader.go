package backupconfiguration

import (
	"context"
	"crypto/sha256"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type captureReadStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

// CaptureSourceEntry contains descriptors and immutable value references only.
// The selected value is loaded separately so aggregate plaintext is bounded.
type CaptureSourceEntry struct {
	Record                  entries.Record
	EntryRevision           int64
	EntrySHA256             [32]byte
	ValueGenerationRevision int64
	ServiceIDs              []string
	SecretID                string
	SecretRevision          int64
	SecretSHA256            [32]byte
	AttachID                string
	AttachRevision          int64
	AttachSHA256            [32]byte
	GrantAttachID           string
	GrantAttachRevision     int64
	GrantAttachSHA256       [32]byte
}

// CaptureSourceReader pins every nested read, including immutable keys, to one
// positive MVCC revision. Compaction fails; it never switches to current state.
type CaptureSourceReader struct {
	store    captureReadStore
	revision int64
}

func NewCaptureSourceReader(store captureReadStore, revision int64) (*CaptureSourceReader, error) {
	if store == nil || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "Config snapshot fixed revision is required")
	}
	return &CaptureSourceReader{store: store, revision: revision}, nil
}

func (reader *CaptureSourceReader) Get(ctx context.Context, key string) (*etcdstore.GetResult, error) {
	read, err := reader.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return nil, err
	}
	return &etcdstore.GetResult{Entry: read.Values[0], ReadRevision: read.ReadRevision}, nil
}

func (reader *CaptureSourceReader) GetMany(
	ctx context.Context,
	request etcdstore.GetManyRequest,
) (*etcdstore.GetManyResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if request.Revision != 0 && request.Revision != reader.revision {
		return nil, captureSourceCorrupt()
	}
	request.Revision = reader.revision
	read, err := reader.store.GetMany(ctx, request)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != reader.revision || len(read.Values) != len(request.Keys) {
		return nil, captureSourceCorrupt()
	}
	return read, nil
}

func (reader *CaptureSourceReader) Range(
	ctx context.Context,
	request etcdstore.RangeRequest,
) (*etcdstore.RangeResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if request.Revision != 0 && request.Revision != reader.revision {
		return nil, captureSourceCorrupt()
	}
	request.Revision = reader.revision
	read, err := reader.store.Range(ctx, request)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != reader.revision {
		return nil, captureSourceCorrupt()
	}
	return read, nil
}

func (reader *CaptureSourceReader) ReadEntries(
	ctx context.Context,
	environmentID string,
) ([]CaptureSourceEntry, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return nil, captureSourceCorrupt()
	}
	env, err := reader.Get(ctx, hierarchy.EnvironmentKey(environmentID))
	if err != nil {
		return nil, err
	}
	if env.Entry == nil {
		return nil, captureSourceCorrupt()
	}
	defer clear(env.Entry.Value)
	environment, err := hierarchy.DecodeEnvironment(env.Entry.Value)
	if err != nil || environment.ID != environmentID {
		return nil, captureSourceCorrupt()
	}
	projection, found, err := environmentqueries.NewProjectionReader(reader).
		GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil || !found {
		return nil, err
	}
	if len(projection.Record.Entries) > backupconfig.MaxEntries {
		return nil, errs.New(errs.KindValidationFailed, "Config snapshot Entry count exceeds its limit")
	}
	result := make([]CaptureSourceEntry, 0, len(projection.Record.Entries))
	for _, record := range projection.Record.Entries {
		entry, err := reader.readEntry(ctx, environmentID, environment.ProjectID, record, projection.Revision)
		if err != nil {
			return nil, err
		}
		result = append(result, entry)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Record.Entry.ID < result[j].Record.Entry.ID })
	return result, nil
}

func (reader *CaptureSourceReader) readEntry(
	ctx context.Context,
	environmentID, projectID string,
	record entries.Record,
	revision int64,
) (CaptureSourceEntry, error) {
	encoded, err := entries.EncodeRecord(record)
	if err != nil {
		return CaptureSourceEntry{}, err
	}
	defer clear(encoded)
	if record.EnvironmentID != environmentID || revision <= 0 {
		return CaptureSourceEntry{}, captureSourceCorrupt()
	}
	entry := CaptureSourceEntry{
		Record:        record,
		EntryRevision: revision,
		EntrySHA256:   sha256.Sum256(encoded),
	}
	value, err := reader.ReadGeneration(ctx, entry)
	if err != nil {
		return CaptureSourceEntry{}, err
	}
	entry.ValueGenerationRevision = value.ModRevision
	clear(value.Value)
	services := environmentqueries.NewServiceReader(reader)
	if !record.Entry.ExposesAll() {
		for _, reference := range record.Entry.Exposure {
			var id string
			if ids.Validate(ids.KindService, reference) == nil {
				selected, err := services.GetService(ctx, reference)
				if err != nil {
					return CaptureSourceEntry{}, err
				}
				if selected.Record.EnvironmentID != environmentID {
					return CaptureSourceEntry{}, captureSourceCorrupt()
				}
				id = selected.Record.Desired.ID
			} else {
				selected, err := services.GetServiceByName(ctx, environmentID, reference)
				if err != nil {
					return CaptureSourceEntry{}, err
				}
				id = selected.Record.Desired.ID
			}
			entry.ServiceIDs = append(entry.ServiceIDs, id)
		}
		sort.Strings(entry.ServiceIDs)
	}
	switch record.Entry.Source.Kind {
	case core.SourceLiteral:
	case core.SourceSecretRef:
		selected, err := secrets.NewReader(reader).
			ResolveSecretAtRevision(ctx, projectID, record.Entry.Source.SecretRef, reader.revision)
		if err != nil {
			return CaptureSourceEntry{}, err
		}
		kind := core.SecretKindEnvVar
		if record.Entry.Kind == core.EntryKindFile {
			kind = core.SecretKindFile
		}
		if !record.Entry.Secret || selected.Record.Secret.Kind != kind {
			return CaptureSourceEntry{}, captureSourceCorrupt()
		}
		raw, err := reader.Get(ctx, secrets.RecordKey(selected.Record.Secret.ID))
		if err != nil {
			return CaptureSourceEntry{}, err
		}
		if raw.Entry == nil || raw.Entry.ModRevision != selected.Revision {
			return CaptureSourceEntry{}, captureSourceCorrupt()
		}
		entry.SecretID, entry.SecretRevision = selected.Record.Secret.ID, selected.Revision
		entry.SecretSHA256 = sha256.Sum256(raw.Entry.Value)
		clear(raw.Entry.Value)
	case core.SourceFact:
		if record.Entry.Source.Fact == nil {
			return CaptureSourceEntry{}, captureSourceCorrupt()
		}
		facts := attachments.NewReader(reader)
		selected, err := facts.ResolveAttach(ctx, environmentID, record.Entry.Source.Fact.Attach)
		if err != nil {
			return CaptureSourceEntry{}, err
		}
		entry.AttachID, entry.AttachRevision = selected.Record.ID, selected.Revision
		entry.AttachSHA256, err = reader.attachDigest(ctx, entry.AttachID, entry.AttachRevision)
		if err != nil {
			return CaptureSourceEntry{}, err
		}
		if record.Entry.Source.Fact.Grant != "" {
			grant, err := facts.ResolveAttach(ctx, environmentID, record.Entry.Source.Fact.Grant)
			if err != nil {
				return CaptureSourceEntry{}, err
			}
			entry.GrantAttachID, entry.GrantAttachRevision = grant.Record.ID, grant.Revision
			entry.GrantAttachSHA256, err = reader.attachDigest(ctx, entry.GrantAttachID, entry.GrantAttachRevision)
			if err != nil {
				return CaptureSourceEntry{}, err
			}
		}
	default:
		return CaptureSourceEntry{}, captureSourceCorrupt()
	}
	return entry, nil
}

func (reader *CaptureSourceReader) attachDigest(ctx context.Context, id string, revision int64) ([32]byte, error) {
	raw, err := reader.Get(ctx, attachments.AttachKey(id))
	if err != nil {
		return [32]byte{}, err
	}
	if raw.Entry == nil || raw.Entry.ModRevision != revision {
		return [32]byte{}, captureSourceCorrupt()
	}
	defer clear(raw.Entry.Value)
	return sha256.Sum256(raw.Entry.Value), nil
}

func (reader *CaptureSourceReader) ReadGeneration(
	ctx context.Context,
	entry CaptureSourceEntry,
) (*etcdstore.KeyValue, error) {
	key := entryvalues.PlainKey(entry.Record.Entry.ID, entry.Record.CurrentValueGenerationID)
	if entry.Record.Entry.Secret {
		key = entryvalues.SecretKey(entry.Record.Entry.ID, entry.Record.CurrentValueGenerationID)
	}
	read, err := reader.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if read.Entry == nil ||
		entry.ValueGenerationRevision > 0 && read.Entry.ModRevision != entry.ValueGenerationRevision {
		return nil, captureSourceCorrupt()
	}
	return read.Entry, nil
}

func captureSourceCorrupt() error {
	return errs.New(errs.KindInternal, "Config snapshot fixed-revision source is incomplete or inconsistent")
}
