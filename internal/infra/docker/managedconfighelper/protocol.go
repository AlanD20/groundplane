// Package managedconfighelper owns the closed protocol and atomic host write
// used by the generic managed-config capability.
package managedconfighelper

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion      = 1
	ManagedRoot        = "/etc/groundplane"
	maximumFramedBytes = managedconfig.MaximumArtifactBytes + 4096
	frameHeaderBytes   = 4
)

func MarshalRequest(request *agentpb.ManagedConfigHelperRequest) ([]byte, error) {
	owned, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(owned.Content)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(encoded) == 0 || len(encoded) > maximumFramedBytes {
		clear(encoded)
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request exceeds its bound")
	}
	framed := make([]byte, frameHeaderBytes+len(encoded))
	binary.BigEndian.PutUint32(framed[:frameHeaderBytes], uint32(len(encoded)))
	copy(framed[frameHeaderBytes:], encoded)
	clear(encoded)
	return framed, nil
}

func ReadRequest(ctx context.Context, input io.Reader) (*agentpb.ManagedConfigHelperRequest, error) {
	if ctx == nil || input == nil {
		return nil, errs.New(errs.KindInternal, "managed-config helper input is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	header := make([]byte, frameHeaderBytes)
	if _, err := io.ReadFull(input, header); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request frame is incomplete")
	}
	size := binary.BigEndian.Uint32(header)
	if size == 0 || size > maximumFramedBytes {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request frame is invalid")
	}
	encoded := make([]byte, int(size))
	defer clear(encoded)
	if _, err := io.ReadFull(input, encoded); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request frame is incomplete")
	}
	trailing := make([]byte, 1)
	if count, err := input.Read(trailing); count != 0 || err == nil || !errors.Is(err, io.EOF) {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request has trailing bytes")
	}
	request := &agentpb.ManagedConfigHelperRequest{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, request); err != nil ||
		len(request.ProtoReflect().GetUnknown()) != 0 {
		clear(request.Content)
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request protobuf is invalid")
	}
	return validateRequest(request)
}

func Apply(ctx context.Context, request *agentpb.ManagedConfigHelperRequest) (resultErr error) {
	if ctx == nil {
		return errs.New(errs.KindInternal, "managed-config helper context is required")
	}
	owned, err := validateRequest(request)
	if err != nil {
		return err
	}
	defer clear(owned.Content)
	if err := ctx.Err(); err != nil {
		return err
	}
	root, err := os.OpenRoot(ManagedRoot)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: open managed root: %w", err))
	}
	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	parent := path.Dir(owned.RelativePath)
	if parent != "." {
		if err := root.MkdirAll(parent, 0o755); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: create destination: %w", err))
		}
		if err := validateDirectoryChain(root, parent); err != nil {
			return err
		}
	}
	if current, inspectErr := root.Lstat(owned.RelativePath); inspectErr == nil {
		if !current.Mode().IsRegular() || current.Mode()&os.ModeSymlink != 0 {
			return errs.New(errs.KindStateConflict, "managed-config destination is not a regular file")
		}
	} else if !errors.Is(inspectErr, os.ErrNotExist) {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: inspect destination: %w", inspectErr))
	}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: create temporary name: %w", err))
	}
	temporary := path.Join(parent, "."+path.Base(owned.RelativePath)+"."+hex.EncodeToString(nonce[:])+".tmp")
	file, err := root.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: create temporary file: %w", err))
	}
	keepTemporary := true
	defer func() {
		if keepTemporary {
			if removeErr := root.Remove(temporary); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
				resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, removeErr))
			}
		}
	}()
	if err := writeAll(ctx, file, owned.Content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: set file mode: %w", err))
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: sync file: %w", err))
	}
	if err := file.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: close file: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := root.Rename(temporary, owned.RelativePath); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: replace destination: %w", err))
	}
	keepTemporary = false
	directory, err := root.Open(parent)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: open destination directory: %w", err))
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(syncErr, closeErr))
	}
	return nil
}

func validateRequest(request *agentpb.ManagedConfigHelperRequest) (*agentpb.ManagedConfigHelperRequest, error) {
	if request == nil || request.GetSchema() != SchemaVersion || !validArtifactID(request.GetArtifactId()) ||
		!validRelativePath(request.GetRelativePath()) || len(request.GetContent()) == 0 ||
		len(request.GetContent()) > managedconfig.MaximumArtifactBytes || len(request.GetSha256()) != sha256.Size ||
		!utf8.Valid(request.GetContent()) || strings.IndexByte(string(request.GetContent()), 0) >= 0 {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request is invalid")
	}
	digest := sha256.Sum256(request.GetContent())
	if subtle.ConstantTimeCompare(digest[:], request.GetSha256()) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper content digest is invalid")
	}
	return proto.Clone(request).(*agentpb.ManagedConfigHelperRequest), nil
}

func validArtifactID(value string) bool {
	if value == "" || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if character < '!' || character > '~' {
			return false
		}
	}
	return true
}

func validRelativePath(value string) bool {
	return value != "" && len(value) <= 240 && !path.IsAbs(value) && path.Clean(value) == value &&
		value != "." && !strings.HasPrefix(value, "../") && !strings.ContainsRune(value, 0)
}

func validateDirectoryChain(root *os.Root, value string) error {
	current := ""
	for _, element := range strings.Split(value, "/") {
		if current == "" {
			current = element
		} else {
			current += "/" + element
		}
		info, err := root.Lstat(current)
		if err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: inspect directory: %w", err))
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errs.New(errs.KindStateConflict, "managed-config destination directory is not trusted")
		}
	}
	return nil
}

func writeAll(ctx context.Context, destination io.Writer, content []byte) error {
	for len(content) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := destination.Write(content)
		if err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("managed-config helper: write file: %w", err))
		}
		if written <= 0 {
			return errs.New(errs.KindInternal, "managed-config helper writer made no progress")
		}
		content = content[written:]
	}
	return nil
}
