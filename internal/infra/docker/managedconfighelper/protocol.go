// Package managedconfighelper owns the closed protocol and transactional host write.
package managedconfighelper

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"syscall"
	"unicode/utf8"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	"github.com/AlanD20/groundplane/internal/common/managedconfig"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	SchemaVersion      = 1
	ManagedRoot        = "/etc/groundplane"
	maximumFramedBytes = managedconfig.MaximumArtifactBytes + 4096
	maximumResponse    = 4096
	frameHeaderBytes   = 4
	transactionRoot    = ".groundplane-transactions"
	lockRoot           = ".groundplane-locks"
	globalLockName     = "managed-config.lock"
	preparationSuffix  = ".preparing"
)

func MarshalRequest(request *agentpb.ManagedConfigHelperRequest) ([]byte, error) {
	owned, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(owned.Content)
	return marshalFrame(owned, maximumFramedBytes)
}
func ReadRequest(ctx context.Context, input io.Reader) (*agentpb.ManagedConfigHelperRequest, error) {
	request := &agentpb.ManagedConfigHelperRequest{}
	if err := readFrame(ctx, input, maximumFramedBytes, request); err != nil {
		return nil, err
	}
	return validateRequest(request)
}
func WriteResponse(output io.Writer, response *agentpb.ManagedConfigHelperResponse) error {
	if output == nil || validateResponse(response) != nil {
		return errs.New(errs.KindInternal, "managed-config helper response is invalid")
	}
	framed, err := marshalFrame(response, maximumResponse)
	if err != nil {
		return err
	}
	defer clear(framed)
	return writeAll(context.Background(), output, framed)
}
func ReadResponse(ctx context.Context, input io.Reader) (*agentpb.ManagedConfigHelperResponse, error) {
	response := &agentpb.ManagedConfigHelperResponse{}
	if err := readFrame(ctx, input, maximumResponse, response); err != nil {
		return nil, err
	}
	if err := validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

func Apply(
	ctx context.Context,
	request *agentpb.ManagedConfigHelperRequest,
) (*agentpb.ManagedConfigHelperResponse, error) {
	return applyAt(ctx, ManagedRoot, request)
}

func applyAt(
	ctx context.Context,
	rootPath string,
	request *agentpb.ManagedConfigHelperRequest,
) (response *agentpb.ManagedConfigHelperResponse, resultErr error) {
	if ctx == nil || rootPath == "" {
		return nil, errs.New(errs.KindInternal, "managed-config helper context is required")
	}
	owned, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	defer clear(owned.Content)
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer func() {
		if closeErr := root.Close(); closeErr != nil {
			resultErr = errs.Wrap(errs.KindInternal, errors.Join(resultErr, closeErr))
		}
	}()
	for _, directory := range []string{path.Dir(owned.RelativePath), transactionRoot, lockRoot} {
		if err := ensureTrustedDirectory(root, directory); err != nil {
			return nil, err
		}
	}
	lock, err := root.OpenFile(
		path.Join(lockRoot, globalLockName),
		os.O_CREATE|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer lock.Close()
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer unix.Flock(int(lock.Fd()), unix.LOCK_UN)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch owned.Operation {
	case agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH:
		return publish(ctx, root, owned)
	case agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT:
		return finalize(ctx, root, owned, true)
	case agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK:
		return finalize(ctx, root, owned, false)
	default:
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper operation is invalid")
	}
}

func publish(
	ctx context.Context,
	root *os.Root,
	request *agentpb.ManagedConfigHelperRequest,
) (*agentpb.ManagedConfigHelperResponse, error) {
	tx := path.Join(transactionRoot, request.TransactionId)
	if info, err := root.Lstat(tx); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, errs.New(errs.KindStateConflict, "managed-config transaction is not trusted")
		}
		return replayPublish(ctx, root, tx, request)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	previous, existed, err := readRegular(root, request.RelativePath)
	if err != nil {
		return nil, err
	}
	defer clear(previous)
	if len(request.ExpectedPreviousSha256) == 0 {
		if existed {
			return nil, errs.New(errs.KindStateConflict, "managed-config predecessor changed")
		}
	} else {
		digest := sha256.Sum256(previous)
		if !existed || subtle.ConstantTimeCompare(digest[:], request.ExpectedPreviousSha256) != 1 {
			return nil, errs.New(errs.KindStateConflict, "managed-config predecessor changed")
		}
	}
	if err := prepareTransaction(ctx, root, tx, request, previous, existed); err != nil {
		return nil, err
	}
	if err := writeAtomic(ctx, root, request.RelativePath, request.Content, 0o644); err != nil {
		return nil, err
	}
	if err := writeAtomic(ctx, root, path.Join(tx, "published"), []byte("published\n"), 0o600); err != nil {
		return nil, err
	}
	return responseFor(
		root,
		request,
		previous,
		existed,
		agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED,
	)
}

func prepareTransaction(
	ctx context.Context,
	root *os.Root,
	tx string,
	request *agentpb.ManagedConfigHelperRequest,
	previous []byte,
	previousExists bool,
) error {
	staging := tx + preparationSuffix
	if err := resetTransactionPreparation(root, staging); err != nil {
		return err
	}
	if err := root.Mkdir(staging, 0o700); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	manifest := proto.Clone(request).(*agentpb.ManagedConfigHelperRequest)
	clear(manifest.Content)
	manifest.Content = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(manifest)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	if err := writeAtomic(ctx, root, path.Join(staging, "request.pb"), encoded, 0o600); err != nil {
		return err
	}
	if err := writeAtomic(ctx, root, path.Join(staging, "candidate"), request.Content, 0o600); err != nil {
		return err
	}
	if previousExists {
		if err := writeAtomic(ctx, root, path.Join(staging, "previous"), previous, 0o600); err != nil {
			return err
		}
	} else if err := writeAtomic(
		ctx,
		root,
		path.Join(staging, "previous.absent"),
		[]byte("absent\n"),
		0o600,
	); err != nil {
		return err
	}
	if err := syncDirectory(root, staging); err != nil {
		return err
	}
	if err := root.Rename(staging, tx); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return syncDirectory(root, transactionRoot)
}

func replayPublish(
	ctx context.Context,
	root *os.Root,
	tx string,
	request *agentpb.ManagedConfigHelperRequest,
) (*agentpb.ManagedConfigHelperResponse, error) {
	manifest, err := readManifest(root, tx)
	if err != nil || !sameTransaction(manifest, request) {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction identity changed")
	}
	committed := exists(root, path.Join(tx, "committed"))
	if exists(root, path.Join(tx, "rolledback")) {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction is already rolled back")
	}
	previous, existed, err := readPrevious(root, tx)
	if err != nil {
		return nil, err
	}
	defer clear(previous)
	live, liveExists, err := readRegular(root, request.RelativePath)
	if err != nil {
		return nil, err
	}
	defer clear(live)
	if committed {
		if !exists(root, path.Join(tx, "published")) || !liveExists || !digestEqual(live, request.Sha256) {
			return nil, errs.New(errs.KindStateConflict, "committed managed-config transaction diverged")
		}
		return responseFor(
			root, request, previous, existed,
			agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY,
		)
	}
	disposition := agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY
	if !liveExists || !digestEqual(live, request.Sha256) {
		if liveExists != existed || liveExists && !bytesEqual(live, previous) {
			return nil, errs.New(errs.KindStateConflict, "managed-config live target diverged during recovery")
		}
		candidate, found, readErr := readRegular(root, path.Join(tx, "candidate"))
		if readErr != nil || !found || !digestEqual(candidate, request.Sha256) {
			clear(candidate)
			return nil, errs.New(errs.KindStateConflict, "managed-config candidate changed during recovery")
		}
		err = writeAtomic(ctx, root, request.RelativePath, candidate, 0o644)
		clear(candidate)
		if err != nil {
			return nil, err
		}
		disposition = agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_RECOVERED
	}
	if !exists(root, path.Join(tx, "published")) {
		if err := writeAtomic(ctx, root, path.Join(tx, "published"), []byte("published\n"), 0o600); err != nil {
			return nil, err
		}
		disposition = agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_RECOVERED
	}
	return responseFor(root, request, previous, existed, disposition)
}

func finalize(
	ctx context.Context,
	root *os.Root,
	request *agentpb.ManagedConfigHelperRequest,
	commit bool,
) (*agentpb.ManagedConfigHelperResponse, error) {
	tx := path.Join(transactionRoot, request.TransactionId)
	manifest, err := readManifest(root, tx)
	if err != nil || !sameTransaction(manifest, request) {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction is unavailable")
	}
	previous, existed, err := readPrevious(root, tx)
	if err != nil {
		return nil, err
	}
	defer clear(previous)
	marker, opposite := "rolledback", "committed"
	if commit {
		marker, opposite = "committed", "rolledback"
	}
	if exists(root, path.Join(tx, opposite)) {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction has the opposite terminal state")
	}
	if exists(root, path.Join(tx, marker)) {
		return responseFor(
			root,
			request,
			previous,
			existed,
			agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_EXACT_REPLAY,
		)
	}
	if commit {
		live, found, readErr := readRegular(root, request.RelativePath)
		if readErr != nil {
			return nil, readErr
		}
		defer clear(live)
		if !found || !digestEqual(live, request.Sha256) {
			return nil, errs.New(errs.KindStateConflict, "managed-config candidate is not live")
		}
	} else if existed {
		if err := writeAtomic(ctx, root, request.RelativePath, previous, 0o644); err != nil {
			return nil, err
		}
	} else {
		if err := root.Remove(request.RelativePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		if err := syncDirectory(root, path.Dir(request.RelativePath)); err != nil {
			return nil, err
		}
	}
	if err := writeAtomic(ctx, root, path.Join(tx, marker), []byte(marker+"\n"), 0o600); err != nil {
		return nil, err
	}
	return responseFor(
		root,
		request,
		previous,
		existed,
		agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_APPLIED,
	)
}

func responseFor(
	root *os.Root,
	request *agentpb.ManagedConfigHelperRequest,
	previous []byte,
	previousExists bool,
	disposition agentpb.ManagedConfigReplayDisposition,
) (*agentpb.ManagedConfigHelperResponse, error) {
	response := &agentpb.ManagedConfigHelperResponse{
		Schema:        SchemaVersion,
		TransactionId: request.TransactionId,
		Operation:     request.Operation,
		Disposition:   disposition,
	}
	if previousExists {
		digest := sha256.Sum256(previous)
		response.PreviousSha256 = append([]byte(nil), digest[:]...)
	}
	live, found, err := readRegular(root, request.RelativePath)
	if err != nil {
		return nil, err
	}
	defer clear(live)
	if found {
		digest := sha256.Sum256(live)
		response.LiveSha256 = append([]byte(nil), digest[:]...)
		file, openErr := root.Open(request.RelativePath)
		if openErr != nil {
			return nil, errs.Wrap(errs.KindInternal, openErr)
		}
		info, statErr := file.Stat()
		closeErr := file.Close()
		if statErr != nil || closeErr != nil {
			return nil, errs.Wrap(errs.KindInternal, errors.Join(statErr, closeErr))
		}
		if stat, ok := info.Sys().(*syscall.Stat_t); ok {
			response.LiveDevice, response.LiveInode = uint64(stat.Dev), stat.Ino
		}
	}
	return response, nil
}

func readManifest(root *os.Root, tx string) (*agentpb.ManagedConfigHelperRequest, error) {
	encoded, found, err := readRegular(root, path.Join(tx, "request.pb"))
	if err != nil || !found {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction manifest is missing")
	}
	defer clear(encoded)
	manifest := &agentpb.ManagedConfigHelperRequest{}
	if err := proto.Unmarshal(encoded, manifest); err != nil {
		return nil, errs.New(errs.KindStateConflict, "managed-config transaction manifest is invalid")
	}
	return manifest, nil
}
func readPrevious(root *os.Root, tx string) ([]byte, bool, error) {
	if exists(root, path.Join(tx, "previous.absent")) {
		return nil, false, nil
	}
	value, found, err := readRegular(root, path.Join(tx, "previous"))
	if err != nil || !found {
		return nil, false, errs.New(errs.KindStateConflict, "managed-config predecessor is missing")
	}
	return value, true, nil
}
func sameTransaction(manifest, request *agentpb.ManagedConfigHelperRequest) bool {
	return manifest != nil && request != nil && manifest.Schema == request.Schema &&
		manifest.ArtifactId == request.ArtifactId &&
		manifest.RelativePath == request.RelativePath &&
		manifest.TransactionId == request.TransactionId &&
		manifest.Generation == request.Generation &&
		subtle.ConstantTimeCompare(manifest.Sha256, request.Sha256) == 1 &&
		subtle.ConstantTimeCompare(manifest.ExpectedPreviousSha256, request.ExpectedPreviousSha256) == 1
}

func validateRequest(request *agentpb.ManagedConfigHelperRequest) (*agentpb.ManagedConfigHelperRequest, error) {
	if request == nil || request.GetSchema() != SchemaVersion || !validArtifactID(request.GetArtifactId()) ||
		!validRelativePath(request.GetRelativePath()) ||
		!validTransactionID(request.GetTransactionId()) ||
		request.GetGeneration() == 0 ||
		len(request.GetSha256()) != sha256.Size ||
		len(request.GetExpectedPreviousSha256()) != 0 && len(request.GetExpectedPreviousSha256()) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper request is invalid")
	}
	switch request.GetOperation() {
	case agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_PUBLISH:
		if len(request.GetContent()) == 0 || len(request.GetContent()) > managedconfig.MaximumArtifactBytes ||
			!utf8.Valid(request.GetContent()) ||
			strings.IndexByte(string(request.GetContent()), 0) >= 0 ||
			!digestEqual(request.GetContent(), request.GetSha256()) {
			return nil, errs.New(errs.KindValidationFailed, "managed-config helper candidate is invalid")
		}
	case agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_COMMIT,
		agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_ROLLBACK:
		if len(request.GetContent()) != 0 {
			return nil, errs.New(errs.KindValidationFailed, "managed-config terminal request carries content")
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper operation is invalid")
	}
	return proto.Clone(request).(*agentpb.ManagedConfigHelperRequest), nil
}
func validateResponse(response *agentpb.ManagedConfigHelperResponse) error {
	if response == nil || response.Schema != SchemaVersion || !validTransactionID(response.TransactionId) ||
		response.Operation == agentpb.ManagedConfigOperation_MANAGED_CONFIG_OPERATION_UNSPECIFIED ||
		response.Disposition == agentpb.ManagedConfigReplayDisposition_MANAGED_CONFIG_REPLAY_DISPOSITION_UNSPECIFIED ||
		len(response.LiveSha256) != 0 && len(response.LiveSha256) != sha256.Size ||
		len(response.PreviousSha256) != 0 && len(response.PreviousSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "managed-config helper response is invalid")
	}
	return nil
}

func marshalFrame(message proto.Message, maximum int) ([]byte, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	if len(encoded) == 0 || len(encoded) > maximum {
		return nil, errs.New(errs.KindValidationFailed, "managed-config helper frame exceeds its bound")
	}
	framed := make([]byte, frameHeaderBytes+len(encoded))
	binary.BigEndian.PutUint32(framed[:frameHeaderBytes], uint32(len(encoded)))
	copy(framed[frameHeaderBytes:], encoded)
	return framed, nil
}
func readFrame(ctx context.Context, input io.Reader, maximum int, message proto.Message) error {
	if ctx == nil || input == nil {
		return errs.New(errs.KindInternal, "managed-config helper input is not configured")
	}
	header := make([]byte, frameHeaderBytes)
	if _, err := io.ReadFull(input, header); err != nil {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame is incomplete")
	}
	size := binary.BigEndian.Uint32(header)
	if size == 0 || size > uint32(maximum) {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame is invalid")
	}
	encoded := make([]byte, int(size))
	defer clear(encoded)
	if _, err := io.ReadFull(input, encoded); err != nil {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame is incomplete")
	}
	trailing := make([]byte, 1)
	if count, err := input.Read(trailing); count != 0 || err == nil || !errors.Is(err, io.EOF) {
		return errs.New(errs.KindValidationFailed, "managed-config helper frame has trailing bytes")
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		encoded,
		message,
	); err != nil ||
		len(message.ProtoReflect().GetUnknown()) != 0 {
		return errs.New(errs.KindValidationFailed, "managed-config helper protobuf is invalid")
	}
	return ctx.Err()
}

func validArtifactID(value string) bool {
	return value != "" && len(value) <= 128 && strings.TrimSpace(value) == value &&
		!strings.ContainsAny(value, "\x00/\\")
}
func validTransactionID(value string) bool {
	if len(value) < 8 || len(value) > 96 {
		return false
	}
	for _, character := range value {
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_') {
			return false
		}
	}
	return true
}
func validRelativePath(value string) bool {
	return value != "" && len(value) <= 240 && !path.IsAbs(value) && path.Clean(value) == value && value != "." &&
		!strings.HasPrefix(value, "../") &&
		!strings.ContainsRune(value, 0)
}
func writeAll(ctx context.Context, destination io.Writer, content []byte) error {
	for len(content) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := destination.Write(content)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if written <= 0 {
			return errs.New(errs.KindInternal, "managed-config helper writer made no progress")
		}
		content = content[written:]
	}
	return nil
}
func digestEqual(content, digest []byte) bool {
	computed := sha256.Sum256(content)
	return len(digest) == sha256.Size && subtle.ConstantTimeCompare(computed[:], digest) == 1
}
func bytesEqual(left, right []byte) bool {
	return len(left) == len(right) && subtle.ConstantTimeCompare(left, right) == 1
}
