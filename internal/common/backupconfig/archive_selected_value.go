package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"unicode/utf8"
)

type valueSink func([]byte, uint64) error

func streamSelectedValue(
	ctx context.Context,
	reader io.Reader,
	entry Entry,
	requireReaderEOF bool,
	sink valueSink,
) ([32]byte, error) {
	if reader == nil {
		return [32]byte{}, archiveError("selected value reader is nil")
	}
	hasher := sha256.New()
	validator := selectedValueValidator{
		requireUTF8: entry.Source.Kind == SourceLiteral ||
			entry.Metadata.Kind == MetadataEnvironment,
		rejectNUL: entry.Metadata.Kind == MetadataEnvironment,
	}
	buffer := make([]byte, TransferChunkBytes)
	defer clearBytes(buffer)
	defer validator.clear()
	remaining := entry.Value.SizeBytes
	var offset uint64
	for remaining != 0 {
		if err := checkContext(ctx); err != nil {
			return [32]byte{}, err
		}
		length := uint64(len(buffer))
		if length > remaining {
			length = remaining
		}
		chunk := buffer[:int(length)]
		if err := readFull(ctx, reader, chunk); err != nil {
			return [32]byte{}, err
		}
		if !validator.consume(chunk) {
			return [32]byte{}, archiveError("selected value violates its UTF-8 or NUL policy")
		}
		// hash.Hash.Write is specified to consume all bytes and never return an error.
		_, _ = hasher.Write(chunk)
		if sink != nil {
			if err := sink(chunk, offset); err != nil {
				return [32]byte{}, err
			}
		}
		offset += length
		remaining -= length
	}
	if !validator.finish() {
		return [32]byte{}, archiveError("selected value ends with incomplete UTF-8")
	}
	if requireReaderEOF {
		if err := checkContext(ctx); err != nil {
			return [32]byte{}, err
		}
		var extra [1]byte
		count, err := reader.Read(extra[:])
		if count != 0 || err == nil {
			return [32]byte{}, archiveError("selected value is longer than its declared size")
		}
		if err != io.EOF {
			return [32]byte{}, archiveCause("selected value EOF probe failed", err)
		}
	}
	var digest [32]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, nil
}

type selectedValueValidator struct {
	requireUTF8 bool
	rejectNUL   bool
	pending     []byte
}

func (validator *selectedValueValidator) consume(value []byte) bool {
	if validator.rejectNUL && bytes.IndexByte(value, 0) >= 0 {
		return false
	}
	if !validator.requireUTF8 {
		return true
	}
	combined := make([]byte, len(validator.pending)+len(value))
	defer clearBytes(combined)
	copy(combined, validator.pending)
	copy(combined[len(validator.pending):], value)
	clearBytes(validator.pending)
	validator.pending = validator.pending[:0]
	index := 0
	for index < len(combined) {
		if !utf8.FullRune(combined[index:]) {
			validator.pending = append(validator.pending, combined[index:]...)
			break
		}
		character, size := utf8.DecodeRune(combined[index:])
		if character == utf8.RuneError && size == 1 {
			return false
		}
		index += size
	}
	return true
}

func (validator *selectedValueValidator) finish() bool {
	return !validator.requireUTF8 || len(validator.pending) == 0
}

func (validator *selectedValueValidator) clear() {
	clearBytes(validator.pending)
	validator.pending = nil
}
