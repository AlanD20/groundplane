package backup

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// PrefixEvidence derives expected retained bytes from the pinned snapshot,
// independently of the Agent's inventory. It reads at most one selected Entry
// at a time and stops at the requested prefix, including partial tar members.
func (reader *CapturedConfigSnapshot) PrefixEvidence(
	ctx context.Context,
	size uint64,
) (_ backupconfig.ArtifactEvidence, resultErr error) {
	if reader == nil || ctx == nil || reader.authority == nil || size > reader.authority.Content.SourceSizeBytes {
		return backupconfig.ArtifactEvidence{}, configSnapshotInvalid()
	}
	if err := ctx.Err(); err != nil {
		return backupconfig.ArtifactEvidence{}, err
	}
	if size == 0 {
		return backupconfig.ArtifactEvidence{SHA256: sha256.Sum256(nil)}, nil
	}
	manifest, content, metadata, err := backupconfig.BuildManifest(
		ctx,
		backupconfig.TransferCapture,
		reader.entries,
		func(ctx context.Context, direction backupconfig.TransferDirection, ordinal uint32, entry backupconfig.Entry) ([]byte, error) {
			value, err := reader.ReadMetadata(ctx, ordinal)
			if err != nil {
				return nil, err
			}
			return proto.MarshalOptions{Deterministic: true}.Marshal(value)
		},
	)
	if err != nil {
		return backupconfig.ArtifactEvidence{}, err
	}
	defer clear(manifest)
	defer func() {
		for _, frame := range metadata {
			clear(frame.CanonicalEntry)
		}
	}()
	layout, err := backupconfig.ComputeLayout(ctx, content, reader.entries)
	if err != nil {
		return backupconfig.ArtifactEvidence{}, err
	}
	values := make([]io.Reader, len(reader.entries))
	owned := make([]*capturedValueReader, len(reader.entries))
	for index := range owned {
		owned[index] = &capturedValueReader{ctx: ctx, snapshot: reader, ordinal: uint32(index + 1)}
		values[index] = owned[index]
	}
	defer func() {
		var cleanupErr error
		for _, value := range owned {
			cleanupErr = errors.Join(cleanupErr, value.Close())
		}
		if cleanupErr != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, cleanupErr)
		}
	}()
	prefix := &configPrefixHasher{limit: size, hasher: sha256.New()}
	err = backupconfig.WriteArtifact(ctx, prefix, manifest, layout, metadata, values)
	if prefix.written != size || (err != nil && !(prefix.stopped && errors.Is(err, io.EOF))) {
		if err != nil {
			return backupconfig.ArtifactEvidence{}, err
		}
		return backupconfig.ArtifactEvidence{}, configSnapshotInvalid()
	}
	if err := ctx.Err(); err != nil {
		return backupconfig.ArtifactEvidence{}, err
	}
	evidence := backupconfig.ArtifactEvidence{SizeBytes: size}
	copy(evidence.SHA256[:], prefix.hasher.Sum(nil))
	return evidence, nil
}

type configPrefixHasher struct {
	limit   uint64
	written uint64
	hasher  hash.Hash
	stopped bool
}

func (writer *configPrefixHasher) WriteAt(content []byte, offset int64) (int, error) {
	if offset < 0 || uint64(offset) != writer.written {
		return 0, configSnapshotInvalid()
	}
	length := uint64(len(content))
	if length > writer.limit-writer.written {
		length = writer.limit - writer.written
	}
	_, _ = writer.hasher.Write(content[:int(length)])
	writer.written += length
	if int(length) != len(content) {
		writer.stopped = true
		return int(length), io.EOF
	}
	return int(length), nil
}

type capturedValueReader struct {
	ctx      context.Context
	snapshot *CapturedConfigSnapshot
	ordinal  uint32
	source   io.ReadCloser
	closed   bool
}

func (reader *capturedValueReader) Read(content []byte) (int, error) {
	if reader.closed {
		return 0, io.EOF
	}
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	if reader.source == nil {
		_, source, err := reader.snapshot.ReadEntry(reader.ctx, reader.ordinal)
		if err != nil {
			return 0, err
		}
		reader.source = source
	}
	count, err := reader.source.Read(content)
	if err == io.EOF {
		if closeErr := reader.Close(); closeErr != nil {
			return count, closeErr
		}
	}
	return count, err
}

func (reader *capturedValueReader) Close() error {
	if reader.closed {
		return nil
	}
	reader.closed = true
	if reader.source == nil {
		return nil
	}
	return reader.source.Close()
}
