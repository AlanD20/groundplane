// Package backupformat owns format-independent Backup byte bounds and exact
// source evidence validation.
package backupformat

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"math"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaxStoredBytes    uint64 = 5 * 1024 * 1024 * 1024 * 1024
	MaxAgeSourceBytes uint64 = 5_496_216_289_016
	AgeChunkBytes     uint64 = 65_536
)

// Evidence authenticates one exact byte stream.
type Evidence struct {
	SizeBytes uint64
	SHA256    [sha256.Size]byte
}

func (evidence Evidence) Validate(maximum uint64) error {
	if maximum > MaxStoredBytes {
		return invalid("backup byte maximum exceeds the stored-object limit")
	}
	if evidence.SizeBytes > maximum {
		return invalid("backup byte stream exceeds its size limit")
	}
	return nil
}

// AgeStoredSize returns the exact unarmored age v1 size for one X25519
// recipient and the canonical 64-KiB payload segmentation.
func AgeStoredSize(sourceSize uint64) (uint64, error) {
	if sourceSize > MaxAgeSourceBytes {
		return 0, invalid("backup age plaintext exceeds its size limit")
	}
	chunks := sourceSize / AgeChunkBytes
	if sourceSize%AgeChunkBytes != 0 {
		chunks++
	}
	if chunks == 0 {
		chunks = 1
	}
	if chunks > (math.MaxUint64-184)/16 {
		return 0, invalid("backup age size arithmetic overflowed")
	}
	overhead := uint64(184) + 16*chunks
	if sourceSize > math.MaxUint64-overhead {
		return 0, invalid("backup age size arithmetic overflowed")
	}
	stored := sourceSize + overhead
	if stored > MaxStoredBytes {
		return 0, invalid("backup age object exceeds the stored-object limit")
	}
	return stored, nil
}

// Hash reads through exact EOF while enforcing maximum before each accepted
// byte. It retains no content.
func Hash(ctx context.Context, source io.Reader, maximum uint64) (Evidence, error) {
	if source == nil {
		return Evidence{}, invalid("backup byte source is required")
	}
	if maximum > MaxStoredBytes {
		return Evidence{}, invalid("backup byte maximum exceeds the stored-object limit")
	}
	digest := sha256.New()
	buffer := make([]byte, 32*1024)
	var size uint64
	for {
		if ctx.Err() != nil {
			return Evidence{}, invalid("backup byte hashing was canceled")
		}
		count, readErr := source.Read(buffer)
		if count > 0 {
			if size > maximum || uint64(count) > maximum-size {
				return Evidence{}, invalid("backup byte stream exceeds its size limit")
			}
			_, _ = digest.Write(buffer[:count])
			size += uint64(count)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return Evidence{}, invalid("backup byte source read failed")
		}
		if count == 0 {
			return Evidence{}, invalid("backup byte source made no progress")
		}
	}
	var result Evidence
	result.SizeBytes = size
	copy(result.SHA256[:], digest.Sum(nil))
	return result, nil
}

// Verify reads exactly the sealed length, requires exact EOF, and compares
// SHA-256 only after the complete stream is consumed.
func Verify(ctx context.Context, source io.Reader, evidence Evidence, maximum uint64) error {
	if source == nil {
		return invalid("backup byte source is required")
	}
	if err := evidence.Validate(maximum); err != nil {
		return err
	}
	digest := sha256.New()
	buffer := make([]byte, 32*1024)
	remaining := evidence.SizeBytes
	for remaining > 0 {
		if ctx.Err() != nil {
			return invalid("backup byte verification was canceled")
		}
		want := uint64(len(buffer))
		if remaining < want {
			want = remaining
		}
		count, readErr := io.ReadFull(source, buffer[:int(want)])
		if readErr != nil || count != int(want) {
			return invalid("backup byte stream is truncated")
		}
		_, _ = digest.Write(buffer[:count])
		remaining -= uint64(count)
	}
	var extra [1]byte
	count, readErr := io.ReadFull(source, extra[:])
	if count != 0 {
		return invalid("backup byte stream has trailing bytes")
	}
	if readErr != io.EOF {
		return invalid("backup byte stream exact-EOF check failed")
	}
	if subtle.ConstantTimeCompare(digest.Sum(nil), evidence.SHA256[:]) != 1 {
		return invalid("backup byte stream digest does not match")
	}
	return nil
}

func invalid(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
