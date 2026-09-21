package backupconfig

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
	"os"
	"sync"
)

const (
	headerNameOffset     = 0
	headerNameLength     = 100
	headerModeOffset     = 100
	headerUIDOffset      = 108
	headerGIDOffset      = 116
	headerSizeOffset     = 124
	headerMTimeOffset    = 136
	headerChecksumOffset = 148
	headerTypeOffset     = 156
	headerMagicOffset    = 257
	headerVersionOffset  = 263
	headerDevMajorOffset = 329
	headerDevMinorOffset = 337
	headerNumericLength  = 8
	headerSizeLength     = 12
	headerChecksumLength = 8
	maximumUSTARSize     = uint64(1<<33 - 1)
)

// ValidatedArtifact is durable parsed-manifest restore authority over an owned,
// unlinked private spool. No caller-owned source, partial digest, or transfer
// cursor is retained here.
type ValidatedArtifact struct {
	mu           sync.Mutex
	spool        ownedSpool
	layout       Layout
	metadata     []MetadataFrame
	sourceSHA256 [32]byte
	closed       bool
	closeErr     error
}

type ownedSpool interface {
	io.ReaderAt
	io.Writer
	io.WriterAt
	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(int64) error
	Close() error
}

type SourceEvidence struct {
	SizeBytes uint64
	SHA256    [32]byte
}

// ValidateArtifact performs restore pass one over every exact source byte.
// It returns only after headers, payloads, padding, footer, manifest evidence,
// source size, and optional source digest all agree.
func ValidateArtifact(
	ctx context.Context,
	source io.ReaderAt,
	evidence SourceEvidence,
	spoolDirectory string,
	encoder MetadataEncoder,
) (artifact *ValidatedArtifact, resultErr error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if source == nil || evidence.SizeBytes < 2*TarBlockBytes ||
		evidence.SizeBytes > MaxSourceBytes ||
		evidence.SHA256 == ([32]byte{}) {
		return nil, archiveError("artifact source or exact size is invalid")
	}
	if err := probeExactEOF(ctx, source, evidence.SizeBytes); err != nil {
		return nil, err
	}
	spool, err := os.CreateTemp(spoolDirectory, ".groundplane-config-validated-*")
	if err != nil {
		return nil, archiveCause("private validated spool creation failed", err)
	}
	name := spool.Name()
	keep := false
	defer func() {
		if !keep {
			resultErr = cleanupOwnedSpool(resultErr, spool)
		}
	}()
	if err := os.Remove(name); err != nil {
		return nil, archiveCause("private validated spool unlink failed", err)
	}
	if err := copyOwnedSource(ctx, spool, source, evidence); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := spool.Sync(); err != nil {
		return nil, archiveCause("private validated spool fsync failed", err)
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	layout, sourceDigest, err := validateOwnedSource(ctx, spool, evidence)
	if err != nil {
		return nil, err
	}
	metadata, err := encodeMetadata(ctx, TransferRestore, layout.Entries, encoder)
	if err != nil {
		return nil, err
	}
	keep = true
	return &ValidatedArtifact{
		spool:        spool,
		layout:       cloneLayout(layout),
		metadata:     cloneMetadataFrames(metadata),
		sourceSHA256: sourceDigest,
	}, nil
}

// Layout returns a defensive copy of the parsed durable content authority and
// member index.
func (artifact *ValidatedArtifact) Layout() Layout {
	if artifact == nil {
		return Layout{}
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	return cloneLayout(artifact.layout)
}

func (artifact *ValidatedArtifact) SourceSHA256() [32]byte {
	if artifact == nil {
		return [32]byte{}
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	return artifact.sourceSHA256
}

// Close clears the private spool authority. Cleanup is attempted even when
// ctx is already canceled so plaintext staging is never retained by mistake.
func (artifact *ValidatedArtifact) Close(ctx context.Context) error {
	if artifact == nil {
		return nil
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	if artifact.closed {
		return artifact.closeErr
	}
	artifact.closed = true
	spool := artifact.spool
	artifact.spool = nil
	for index := range artifact.metadata {
		clearBytes(artifact.metadata[index].CanonicalEntry)
	}
	artifact.metadata = nil
	artifact.closeErr = cleanupOwnedSpool(checkContext(ctx), spool)
	return artifact.closeErr
}

func cleanupOwnedSpool(primary error, spool ownedSpool) error {
	if spool == nil {
		return primary
	}
	causes := make([]error, 0, 4)
	if primary != nil {
		causes = append(causes, primary)
	}
	if err := spool.Truncate(0); err != nil {
		causes = append(causes, fmt.Errorf("private validated spool truncate: %w", err))
	}
	if err := spool.Sync(); err != nil {
		causes = append(causes, fmt.Errorf("private validated spool cleanup fsync: %w", err))
	}
	// os.File.Close documents that a returned error does not make a second
	// Close a valid recovery operation. This is the one terminal close attempt.
	if err := spool.Close(); err != nil {
		causes = append(causes, fmt.Errorf("private validated spool close: %w", err))
	}
	if len(causes) == 0 {
		return nil
	}
	if len(causes) == 1 && primary != nil {
		return primary
	}
	if primary == nil {
		causes = append([]error{errors.New("private validated spool cleanup failed")}, causes...)
	}
	return errs.WrapJoined(errs.KindInternal, causes[0], causes[1:]...)
}
