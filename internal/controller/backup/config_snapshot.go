package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"hash"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type configSnapshotSourceFactory func(int64) (*backupconfiguration.CaptureSourceReader, error)

// ConfigSnapshotProducer prepares complete authority without durable writes.
// CapturePublished writes only after Task publication has atomically claimed a
// Building cursor and the Environment mutation fence. That fence protects the
// selected generations from ordinary Entry changes until capture releases it.
type ConfigSnapshotProducer struct {
	sources   configSnapshotSourceFactory
	protector *secretvalue.Protector
	snapshots *backupconfiguration.CaptureSnapshotRepository
}

func NewConfigSnapshotProducer(
	store etcdstore.Store,
	protector *secretvalue.Protector,
) (*ConfigSnapshotProducer, error) {
	if store == nil || protector == nil {
		return nil, errs.New(errs.KindInternal, "Config snapshot producer dependencies are required")
	}
	guard, err := NewConfigSnapshotWriteGuard(backupruntime.NewReader(store))
	if err != nil {
		return nil, err
	}
	snapshots, err := backupconfiguration.NewCaptureSnapshotRepository(store, guard)
	if err != nil {
		return nil, err
	}
	sources := func(revision int64) (*backupconfiguration.CaptureSourceReader, error) {
		return backupconfiguration.NewCaptureSourceReader(store, revision)
	}
	return &ConfigSnapshotProducer{sources: sources, protector: protector, snapshots: snapshots}, nil
}

// ResolveConfigSnapshot is the BackupConfigSnapshotResolver preparation seam.
// Its evidence comes from actual values at ReadRevision, not a Building cursor.
// Durable capture must verify this same authority before sealing the snapshot.
func (producer *ConfigSnapshotProducer) ResolveConfigSnapshot(
	ctx context.Context,
	input backupplanning.BackupConfigSnapshotInput,
) (*agentpb.BackupConfigCaptureAuthority, error) {
	prepared, err := producer.prepareConfigSnapshot(ctx, input)
	if err != nil {
		return nil, err
	}
	defer prepared.clear()
	return prepared.authority, nil
}

type preparedConfigSnapshot struct {
	reader       *backupconfiguration.CaptureSourceReader
	sources      []backupconfiguration.CaptureSourceEntry
	entries      []backupconfig.Entry
	metadata     []backupconfig.MetadataFrame
	manifest     []byte
	authority    *agentpb.BackupConfigCaptureAuthority
	sourceSHA256 [32]byte
}

func (prepared *preparedConfigSnapshot) clear() {
	clear(prepared.manifest)
	for _, frame := range prepared.metadata {
		clear(frame.CanonicalEntry)
	}
}

func (producer *ConfigSnapshotProducer) prepareConfigSnapshot(
	ctx context.Context,
	input backupplanning.BackupConfigSnapshotInput,
) (_ *preparedConfigSnapshot, resultErr error) {
	if ctx == nil || producer == nil || input.ReadRevision <= 0 ||
		ids.Validate(ids.KindTask, input.TaskID) != nil ||
		ids.Validate(ids.KindTask, input.SnapshotID) != nil ||
		ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
		ids.Validate(ids.KindBackupSource, input.SourceID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Config snapshot context and fixed revision are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := producer.sources(input.ReadRevision)
	if err != nil {
		return nil, err
	}
	sources, err := reader.ReadEntries(ctx, input.EnvironmentID)
	if err != nil {
		return nil, err
	}
	prepared := &preparedConfigSnapshot{
		reader:  reader,
		sources: sources,
		entries: make([]backupconfig.Entry, len(sources)),
	}
	defer func() {
		if resultErr != nil {
			prepared.clear()
		}
	}()
	for index, source := range sources {
		entry, err := configSnapshotEntry(source)
		if err != nil {
			return nil, err
		}
		err = producer.withConfigSnapshotValue(ctx, reader, source, func(value []byte) error {
			if len(value) > backupconfig.MaxSelectedValueBytes {
				return configSnapshotInvalid()
			}
			entry.Value = backupconfig.ValueEvidence{
				Path:      "values/" + entry.ID,
				SizeBytes: uint64(len(value)),
				SHA256:    sha256.Sum256(value),
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		prepared.entries[index] = entry
	}
	encoder := func(ctx context.Context, direction backupconfig.TransferDirection, ordinal uint32, entry backupconfig.Entry) ([]byte, error) {
		if direction != backupconfig.TransferCapture || ordinal == 0 || int(ordinal) > len(sources) ||
			sources[ordinal-1].Record.Entry.ID != entry.ID {
			return nil, configSnapshotInvalid()
		}
		metadata := configSnapshotMetadata(ordinal, sources[ordinal-1], entry)
		if len(metadata.ProtoReflect().GetUnknown()) != 0 {
			return nil, configSnapshotInvalid()
		}
		return proto.MarshalOptions{Deterministic: true}.Marshal(metadata)
	}
	manifest, authority, frames, err := backupconfig.BuildManifest(
		ctx,
		backupconfig.TransferCapture,
		prepared.entries,
		encoder,
	)
	if err != nil {
		return nil, err
	}
	prepared.manifest, prepared.metadata = manifest, frames
	metadataSHA, metadataBytes, err := backupconfig.MetadataSnapshotSHA256(ctx, prepared.entries, frames)
	if err != nil {
		return nil, err
	}
	prepared.authority = &agentpb.BackupConfigCaptureAuthority{EnvironmentId: input.EnvironmentID,
		MetadataSnapshotRevision: input.ReadRevision, MetadataEntryCount: authority.EntryCount, MetadataProtoBytes: metadataBytes,
		Content: &agentpb.BackupConfigContentAuthority{
			ManifestSha256:          append([]byte(nil), authority.ManifestSHA256[:]...),
			EntryCount:              authority.EntryCount,
			TotalSelectedValueBytes: authority.TotalSelectedValueBytes,
			ManifestSizeBytes:       authority.ManifestSizeBytes,
			SourceSizeBytes:         authority.SourceSizeBytes,
			MetadataSnapshotSha256:  append([]byte(nil), metadataSHA[:]...),
		}}
	layout, err := backupconfig.ComputeLayout(ctx, authority, prepared.entries)
	if err != nil {
		return nil, err
	}
	values := make([]io.Reader, len(sources))
	readers := make([]*configSnapshotValueReader, len(sources))
	defer func() {
		for _, value := range readers {
			if value != nil {
				value.clear()
			}
		}
	}()
	for index, source := range sources {
		readers[index] = &configSnapshotValueReader{ctx: ctx, producer: producer, reader: reader, source: source}
		values[index] = readers[index]
	}
	hasher := &configSnapshotHashWriter{hash: sha256.New()}
	if err := backupconfig.WriteArtifact(ctx, hasher, manifest, layout, frames, values); err != nil {
		return nil, err
	}
	if hasher.offset != int64(authority.SourceSizeBytes) {
		return nil, configSnapshotInvalid()
	}
	copy(prepared.sourceSHA256[:], hasher.hash.Sum(nil))
	return prepared, nil
}

func configSnapshotEntry(source backupconfiguration.CaptureSourceEntry) (backupconfig.Entry, error) {
	desired := source.Record.Entry
	entry := backupconfig.Entry{ID: desired.ID, Secret: desired.Secret}
	if desired.Kind == core.EntryKindEnv {
		entry.Metadata = backupconfig.Metadata{
			Kind:        backupconfig.MetadataEnvironment,
			Environment: backupconfig.EnvironmentMetadata{Key: desired.Key},
		}
	} else if desired.Kind == core.EntryKindFile && desired.UID != nil && desired.GID != nil {
		mode := uint32(0444)
		if desired.Secret {
			mode = 0600
		}
		entry.Metadata = backupconfig.Metadata{Kind: backupconfig.MetadataFile, File: backupconfig.FileMetadata{Path: desired.Path, Mode: mode, UID: *desired.UID, GID: *desired.GID}}
	} else {
		return backupconfig.Entry{}, configSnapshotInvalid()
	}
	entry.Exposure = backupconfig.Exposure{Kind: backupconfig.ExposureAll}
	if !desired.ExposesAll() {
		entry.Exposure = backupconfig.Exposure{
			Kind:       backupconfig.ExposureServices,
			ServiceIDs: append([]string(nil), source.ServiceIDs...),
		}
	}
	switch desired.Source.Kind {
	case core.SourceLiteral:
		entry.Source.Kind = backupconfig.SourceLiteral
	case core.SourceSecretRef:
		entry.Source = backupconfig.Source{
			Kind:            backupconfig.SourceSecretReference,
			SecretReference: backupconfig.SecretReference{AuthoredKey: desired.Source.SecretRef},
		}
	case core.SourceFact:
		if desired.Source.Fact == nil {
			return backupconfig.Entry{}, configSnapshotInvalid()
		}
		entry.Source = backupconfig.Source{
			Kind: backupconfig.SourceFact,
			Fact: backupconfig.FactReference{
				AttachID:      source.AttachID,
				Fact:          desired.Source.Fact.Key,
				GrantAttachID: source.GrantAttachID,
			},
		}
	default:
		return backupconfig.Entry{}, configSnapshotInvalid()
	}
	return entry, nil
}

func configSnapshotMetadata(
	ordinal uint32,
	source backupconfiguration.CaptureSourceEntry,
	entry backupconfig.Entry,
) *agentpb.BackupConfigEntry {
	secret := entry.Secret
	metadata := &agentpb.BackupConfigEntry{Ordinal: ordinal, EntryId: entry.ID, Secret: &secret,
		Entry: configSnapshotRevision(
			source.EntryRevision,
			source.EntrySHA256,
		), SelectedValueSizeBytes: entry.Value.SizeBytes,
		SelectedValueSha256: append([]byte(nil), entry.Value.SHA256[:]...)}
	if entry.Metadata.Kind == backupconfig.MetadataEnvironment {
		metadata.Metadata = &agentpb.BackupConfigEntry_Environment{
			Environment: &agentpb.BackupConfigEnvironment{Name: entry.Metadata.Environment.Key},
		}
	} else {
		file := entry.Metadata.File
		metadata.Metadata = &agentpb.BackupConfigEntry_File{File: &agentpb.BackupConfigFile{Path: file.Path, Mode: file.Mode, Uid: file.UID, Gid: file.GID}}
	}
	if entry.Exposure.Kind == backupconfig.ExposureAll {
		metadata.Exposure = &agentpb.BackupConfigEntry_ExposureAll{ExposureAll: &agentpb.BackupConfigExposureAll{}}
	} else {
		metadata.Exposure = &agentpb.BackupConfigEntry_ExposureServices{ExposureServices: &agentpb.BackupConfigExposureServices{ServiceIds: append([]string(nil), entry.Exposure.ServiceIDs...)}}
	}
	switch entry.Source.Kind {
	case backupconfig.SourceLiteral:
		metadata.Source = &agentpb.BackupConfigEntry_Literal{Literal: &agentpb.BackupConfigLiteral{}}
	case backupconfig.SourceSecretReference:
		metadata.Source = &agentpb.BackupConfigEntry_SecretRef{
			SecretRef: &agentpb.BackupConfigSecretRef{
				SecretId:    source.SecretID,
				AuthoredKey: entry.Source.SecretReference.AuthoredKey,
				Secret:      configSnapshotRevision(source.SecretRevision, source.SecretSHA256),
			},
		}
	case backupconfig.SourceFact:
		fact := &agentpb.BackupConfigFactRef{
			AttachId: source.AttachID,
			Fact:     entry.Source.Fact.Fact,
			Attach:   configSnapshotRevision(source.AttachRevision, source.AttachSHA256),
		}
		if source.GrantAttachID != "" {
			id := source.GrantAttachID
			fact.GrantAttachId = &id
			fact.GrantAttach = configSnapshotRevision(source.GrantAttachRevision, source.GrantAttachSHA256)
		}
		metadata.Source = &agentpb.BackupConfigEntry_FactRef{FactRef: fact}
	}
	return metadata
}

func configSnapshotRevision(revision int64, digest [32]byte) *agentpb.RevisionDigest {
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: append([]byte(nil), digest[:]...)}
}

func (producer *ConfigSnapshotProducer) withConfigSnapshotValue(
	ctx context.Context,
	reader *backupconfiguration.CaptureSourceReader,
	source backupconfiguration.CaptureSourceEntry,
	consume secretvalue.PlaintextConsumer,
) error {
	raw, err := reader.ReadGeneration(ctx, source)
	if err != nil {
		return err
	}
	defer clear(raw.Value)
	if !source.Record.Entry.Secret {
		value, err := entryvalues.DecodePlain(raw.Value)
		defer clear(value.Content)
		if err != nil || value.EnvironmentID != source.Record.EnvironmentID ||
			value.EntryID != source.Record.Entry.ID ||
			value.GenerationID != source.Record.CurrentValueGenerationID {
			return configSnapshotInvalid()
		}
		return consume(value.Content)
	}
	value, err := entryvalues.DecodeSecret(raw.Value)
	if err != nil || value.EnvironmentID != source.Record.EnvironmentID || value.EntryID != source.Record.Entry.ID ||
		value.GenerationID != source.Record.CurrentValueGenerationID {
		clear(value.Ciphertext)
		return configSnapshotInvalid()
	}
	envelope, err := secretvalue.RestoreOwned(
		secretvalue.Metadata{
			Version: secretvalue.EnvelopeVersion(value.EnvelopeVersion),
			Cipher:  secretvalue.CipherSuite(value.Cipher),
			Digest: secretvalue.Digest{
				Algorithm: secretvalue.DigestAlgorithm(value.DigestAlgorithm),
				Value:     value.CiphertextSHA256,
			},
		},
		value.Ciphertext,
	)
	value.Ciphertext = nil
	if err != nil {
		return err
	}
	defer envelope.Clear()
	return producer.protector.OpenOwned(ctx, &envelope, consume)
}

type configSnapshotValueReader struct {
	ctx      context.Context
	producer *ConfigSnapshotProducer
	reader   *backupconfiguration.CaptureSourceReader
	source   backupconfiguration.CaptureSourceEntry
	value    []byte
	position int
	loaded   bool
}

func (reader *configSnapshotValueReader) Read(target []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		reader.clear()
		return 0, err
	}
	if !reader.loaded {
		reader.loaded = true
		err := reader.producer.withConfigSnapshotValue(
			reader.ctx,
			reader.reader,
			reader.source,
			func(value []byte) error { reader.value = append([]byte(nil), value...); return nil },
		)
		if err != nil {
			reader.clear()
			return 0, err
		}
	}
	if reader.position == len(reader.value) {
		reader.clear()
		return 0, io.EOF
	}
	count := copy(target, reader.value[reader.position:])
	reader.position += count
	return count, nil
}

func (reader *configSnapshotValueReader) clear() {
	clear(reader.value)
	reader.value = nil
	reader.position = 0
}

type configSnapshotHashWriter struct {
	hash   hash.Hash
	offset int64
}

func (writer *configSnapshotHashWriter) WriteAt(value []byte, offset int64) (int, error) {
	if offset != writer.offset {
		return 0, configSnapshotInvalid()
	}
	count, err := writer.hash.Write(value)
	writer.offset += int64(count)
	return count, err
}

func configSnapshotInvalid() error {
	return errs.New(errs.KindInternal, "Config snapshot content or immutable value evidence is inconsistent")
}

func configSnapshotSameAuthority(left, right *agentpb.BackupConfigCaptureAuthority) bool {
	return left != nil && right != nil && left.Content != nil && right.Content != nil && proto.Equal(left, right) &&
		bytes.Equal(left.Content.MetadataSnapshotSha256, right.Content.MetadataSnapshotSha256)
}
