// Package runnerproxy provides admission checks for workflow access to a
// dedicated rootless Docker daemon. It is not yet wired into the host runtime.
package runnerproxy

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumContextInput    = 512 << 20
	maximumContextExpanded = 1 << 30
	maximumContextEntries  = 100000
	maximumContextPath     = 4096
)

// BuildContext retains only a validated, normalized tar stream. The temporary
// file is unlinked immediately: closing the descriptor releases all scratch.
// A context is owned by one serialized build, not shared between requests.
type BuildContext struct {
	file    *os.File
	size    int64
	digest  string
	entries map[string]contextEntry
}

// PrepareBuildContext must finish before the caller opens a Docker build request.
// spoolRoot is the release-owned private proxy directory, never a workflow bind.
func PrepareBuildContext(ctx context.Context, spoolRoot string, input io.Reader) (*BuildContext, error) {
	if ctx == nil || input == nil {
		return nil, errs.New(errs.KindValidationFailed, "Runner build context input is missing")
	}
	metadata, err := os.Lstat(spoolRoot)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if !metadata.IsDir() || metadata.Mode().Perm() != 0o700 {
		return nil, errs.New(errs.KindStateConflict, "Runner build spool must be a private directory")
	}
	file, err := os.CreateTemp(spoolRoot, "build-context-*")
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := os.Remove(file.Name()); err != nil {
		return nil, errs.Wrap(errs.KindInternal, errors.Join(err, file.Close()))
	}
	result, err := spoolBuildContext(ctx, file, input)
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			return nil, errs.Wrap(errs.KindInternal, errors.Join(err, closeErr))
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	return result, nil
}

func spoolBuildContext(ctx context.Context, file *os.File, input io.Reader) (*BuildContext, error) {
	encoded := &contextReader{ctx: ctx, reader: input, remaining: maximumContextInput}
	buffered := bufio.NewReader(encoded)
	magic, err := buffered.Peek(2)
	if err != nil {
		return nil, invalidContext()
	}
	var source io.Reader = buffered
	var compressed *gzip.Reader
	if magic[0] == 0x1f && magic[1] == 0x8b {
		compressed, err = gzip.NewReader(buffered)
		if err != nil {
			return nil, invalidContext()
		}
		compressed.Multistream(false)
		defer compressed.Close()
		source = compressed
	}
	expanded := &contextReader{ctx: ctx, reader: source, remaining: maximumContextExpanded}
	digest := sha256.New()
	output := &contextWriter{ctx: ctx, writer: io.MultiWriter(file, digest), remaining: maximumContextExpanded}
	writer := tar.NewWriter(output)
	entries, err := normalizeContext(tar.NewReader(expanded), writer)
	if err != nil {
		return nil, err
	}
	// tar.Reader stops at its terminator. Consume padding and verify the gzip
	// checksum, rejecting another archive hidden after that terminator.
	padding := make([]byte, 32<<10)
	for {
		n, readErr := expanded.Read(padding)
		for _, value := range padding[:n] {
			if value != 0 {
				return nil, invalidContext()
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, invalidContext()
		}
	}
	if compressed != nil {
		if _, err := buffered.Peek(1); !errors.Is(err, io.EOF) {
			return nil, invalidContext()
		}
	}
	if err := validateContextLinks(entries); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Sync(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &BuildContext{
		file: file, size: maximumContextExpanded - output.remaining,
		digest: "sha256:" + hex.EncodeToString(digest.Sum(nil)), entries: entries,
	}, nil
}

func normalizeContext(reader *tar.Reader, writer *tar.Writer) (map[string]contextEntry, error) {
	entries := make(map[string]contextEntry)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || len(entries) >= maximumContextEntries {
			return nil, invalidContext()
		}
		name, err := contextPath(header.Name)
		if err != nil {
			return nil, err
		}
		if _, duplicate := entries[name]; duplicate || header.Mode & ^int64(0o777) != 0 {
			return nil, invalidContext()
		}
		if header.Size < 0 || header.Size > maximumContextExpanded {
			return nil, invalidContext()
		}
		for key := range header.PAXRecords {
			switch key {
			case "path", "linkpath", "mtime", "atime", "ctime", "size", "uid", "gid", "uname", "gname":
			default:
				return nil, invalidContext()
			}
		}
		entry := contextEntry{kind: header.Typeflag}
		switch header.Typeflag {
		case tar.TypeReg:
			entry.kind = tar.TypeReg
		case tar.TypeDir:
			if header.Size != 0 || header.Linkname != "" {
				return nil, invalidContext()
			}
		case tar.TypeSymlink, tar.TypeLink:
			if header.Size != 0 || !validLinkText(header.Linkname) {
				return nil, invalidContext()
			}
			entry.link = header.Linkname
		default:
			return nil, invalidContext()
		}
		if name == "." && entry.kind != tar.TypeDir {
			return nil, invalidContext()
		}
		entries[name] = entry
		canonical := &tar.Header{
			Name: name, Typeflag: entry.kind, Linkname: entry.link,
			Mode: header.Mode, Size: header.Size, ModTime: header.ModTime, Format: tar.FormatPAX,
		}
		if err := writer.WriteHeader(canonical); err != nil {
			return nil, invalidContext()
		}
		if _, err := io.Copy(writer, reader); err != nil {
			return nil, invalidContext()
		}
	}
	if len(entries) == 0 {
		return nil, invalidContext()
	}
	return entries, nil
}

func (build *BuildContext) Read(value []byte) (int, error) { return build.file.Read(value) }
func (build *BuildContext) Close() error                   { return build.file.Close() }
func (build *BuildContext) Size() int64                    { return build.size }
func (build *BuildContext) Digest() string                 { return build.digest }

func (build *BuildContext) HasDockerfile(name string) bool {
	canonical, err := contextPath(name)
	entry, found := build.entries[canonical]
	return err == nil && found && entry.kind == tar.TypeReg
}

type contextReader struct {
	ctx       context.Context
	reader    io.Reader
	remaining int64
}

func (reader *contextReader) Read(value []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	if reader.remaining < 0 {
		return 0, invalidContext()
	}
	if int64(len(value)) > reader.remaining+1 {
		value = value[:reader.remaining+1]
	}
	n, err := reader.reader.Read(value)
	reader.remaining -= int64(n)
	if reader.remaining < 0 {
		return 0, invalidContext()
	}
	return n, err
}

type contextWriter struct {
	ctx       context.Context
	writer    io.Writer
	remaining int64
}

func (writer *contextWriter) Write(value []byte) (int, error) {
	if err := writer.ctx.Err(); err != nil {
		return 0, err
	}
	if int64(len(value)) > writer.remaining {
		return 0, invalidContext()
	}
	n, err := writer.writer.Write(value)
	writer.remaining -= int64(n)
	return n, err
}

func invalidContext() error {
	return errs.New(errs.KindValidationFailed, "Runner build context is invalid or exceeds its limits")
}

func validLinkText(value string) bool {
	return value != "" && len(value) <= maximumContextPath &&
		!strings.HasPrefix(value, "/") && !strings.ContainsAny(value, "\\\x00\r\n:")
}
