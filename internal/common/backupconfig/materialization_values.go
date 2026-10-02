package backupconfig

import (
	"context"
	"crypto/sha256"
)

// MaterializationValues is a read-only view of a fully authenticated spool.
// Unlike transfer PassTwo, it has no publication cursor: the selected generation
// is already durable before host files are written. Each read returns owned
// bytes which the caller must clear, never a borrowed spool buffer.
type MaterializationValues struct {
	artifact *ValidatedArtifact
	layout   Layout
}

func (artifact *ValidatedArtifact) MaterializationValues(ctx context.Context) (*MaterializationValues, error) {
	if artifact == nil {
		return nil, archiveError("validated artifact is nil")
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	if artifact.closed || artifact.spool == nil {
		return nil, archiveError("validated artifact is closed")
	}
	layout, digest, err := validateOwnedSource(ctx, artifact.spool, SourceEvidence{
		SizeBytes: artifact.layout.Authority.SourceSizeBytes, SHA256: artifact.sourceSHA256})
	if err != nil {
		return nil, err
	}
	same, err := sameLayoutContent(ctx, layout, artifact.layout)
	if err != nil || !same || digest != artifact.sourceSHA256 {
		return nil, archiveError("materialization source changed after validation")
	}
	return &MaterializationValues{artifact: artifact, layout: cloneLayout(layout)}, nil
}

func (values *MaterializationValues) Entries() []Entry {
	if values == nil {
		return nil
	}
	return cloneEntries(values.layout.Entries)
}

func (values *MaterializationValues) ReadValue(ctx context.Context, index int) (Entry, []byte, error) {
	if values == nil || values.artifact == nil {
		return Entry{}, nil, archiveError("materialization source is nil")
	}
	if err := checkContext(ctx); err != nil {
		return Entry{}, nil, err
	}
	values.artifact.mu.Lock()
	defer values.artifact.mu.Unlock()
	return values.artifact.readValidatedValue(ctx, values.layout, index)
}

// Caller holds the artifact mutex. The same reader policy serves transfer and
// materialization; neither consumer can skip exact digest/UTF-8/NUL validation.
func (artifact *ValidatedArtifact) readValidatedValue(
	ctx context.Context,
	layout Layout,
	index int,
) (Entry, []byte, error) {
	if artifact.closed || artifact.spool == nil || index < 0 || index >= len(layout.Entries) {
		return Entry{}, nil, archiveError("validated value is unavailable")
	}
	entry := cloneEntries([]Entry{layout.Entries[index]})[0]
	value := make([]byte, int(entry.Value.SizeBytes))
	if err := readAtContext(ctx, artifact.spool, value, layout.ValuePayloadOffsets[index]); err != nil {
		clearBytes(value)
		return Entry{}, nil, err
	}
	validator := selectedValueValidator{
		requireUTF8: entry.Source.Kind == SourceLiteral || entry.Metadata.Kind == MetadataEnvironment,
		rejectNUL:   entry.Metadata.Kind == MetadataEnvironment,
	}
	valid := validator.consume(value) && validator.finish()
	validator.clear()
	if !valid || sha256.Sum256(value) != entry.Value.SHA256 {
		clearBytes(value)
		return Entry{}, nil, archiveError("validated selected value changed after authentication")
	}
	return entry, value, nil
}
