package s3compatible

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"hash"
	"io"
)

func digestRange(
	ctx context.Context,
	source io.ReaderAt,
	offset uint64,
	length uint64,
	whole hash.Hash,
) (string, [sha256.Size]byte, error) {
	md5Hash := md5.New()
	writers := []io.Writer{md5Hash}
	localSHA256 := sha256.New()
	if whole == nil {
		writers = append(writers, localSHA256)
	} else {
		writers = append(writers, whole)
	}
	writer := io.MultiWriter(writers...)
	const maximumRead = 64 * 1024
	buffer := make([]byte, maximumRead)
	for copied := uint64(0); copied < length; {
		if err := ctx.Err(); err != nil {
			return "", [sha256.Size]byte{}, err
		}
		readLength := min(uint64(len(buffer)), length-copied)
		n, readErr := source.ReadAt(buffer[:int(readLength)], int64(offset+copied))
		if n > 0 {
			written, writeErr := writer.Write(buffer[:n])
			if writeErr != nil || written != n {
				return "", [sha256.Size]byte{}, internalError()
			}
			copied += uint64(n)
		}
		if n != int(readLength) || readErr != nil && !errors.Is(readErr, io.EOF) {
			return "", [sha256.Size]byte{}, internalError()
		}
	}
	var digest [sha256.Size]byte
	if whole == nil {
		copy(digest[:], localSHA256.Sum(nil))
	}
	return base64.StdEncoding.EncodeToString(md5Hash.Sum(nil)), digest, nil
}
