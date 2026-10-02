package backupconfigtransfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prefixReplayWriter compares canonical output against all existing bytes and
// appends only after them. Losing an acknowledgement does not authorize tail
// truncation; the same sealed input must reproduce that tail before publication.
type prefixReplayWriter struct {
	ctx         context.Context
	prefix      io.ReaderAt
	evidence    backupconfig.ArtifactEvidence
	destination io.Writer
	offset      uint64
}

func newPrefixReplayWriter(
	ctx context.Context,
	prefix io.ReaderAt,
	evidence backupconfig.ArtifactEvidence,
	destination io.Writer,
) (*prefixReplayWriter, error) {
	if ctx == nil || destination == nil || evidence.SizeBytes > backupconfig.MaxSourceBytes ||
		(evidence.SizeBytes > 0 && prefix == nil) {
		return nil, invalid("Config retained prefix is invalid")
	}
	// Exact Controller disposition and the staging descriptor owner supply this
	// evidence. Recheck bytes here before construction; no pathname is reopened.
	hasher := sha256.New()
	buffer := make([]byte, backupconfig.TransferChunkBytes)
	defer clear(buffer)
	for offset := uint64(0); offset < evidence.SizeBytes; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		length := uint64(len(buffer))
		if length > evidence.SizeBytes-offset {
			length = evidence.SizeBytes - offset
		}
		chunk := buffer[:int(length)]
		count, err := prefix.ReadAt(chunk, int64(offset))
		if count != len(chunk) || (err != nil && err != io.EOF) {
			return nil, invalid("Config retained prefix is incomplete")
		}
		_, _ = hasher.Write(chunk)
		offset += length
	}
	if !sameDigest(hasher.Sum(nil), evidence.SHA256[:]) {
		return nil, invalid("Config retained prefix digest changed")
	}
	return &prefixReplayWriter{ctx: ctx, prefix: prefix, evidence: evidence, destination: destination}, nil
}

func (writer *prefixReplayWriter) Write(content []byte) (int, error) {
	if err := writer.ctx.Err(); err != nil {
		return 0, err
	}
	var compared int
	if writer.offset < writer.evidence.SizeBytes {
		length := uint64(len(content))
		if length > writer.evidence.SizeBytes-writer.offset {
			length = writer.evidence.SizeBytes - writer.offset
		}
		buffer := make([]byte, backupconfig.TransferChunkBytes)
		defer clear(buffer)
		for uint64(compared) < length {
			if err := writer.ctx.Err(); err != nil {
				return compared, err
			}
			bound := uint64(len(buffer))
			if bound > length-uint64(compared) {
				bound = length - uint64(compared)
			}
			chunk := buffer[:int(bound)]
			count, err := writer.prefix.ReadAt(chunk, int64(writer.offset))
			if count != len(chunk) || (err != nil && err != io.EOF) ||
				!bytes.Equal(chunk, content[compared:compared+len(chunk)]) {
				return compared, invalid("Config replay differs from the retained archive prefix")
			}
			compared += len(chunk)
			writer.offset += bound
		}
		content = content[compared:]
	}
	if len(content) == 0 {
		return compared, nil
	}
	count, err := writer.destination.Write(content)
	if count < 0 || count > len(content) {
		return compared, errs.New(errs.KindInternal, "Config stage returned an invalid append count")
	}
	writer.offset += uint64(count)
	return compared + count, err
}

func (writer *prefixReplayWriter) consumed() bool { return writer.offset >= writer.evidence.SizeBytes }
