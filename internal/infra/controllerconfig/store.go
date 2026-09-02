// Package controllerconfig owns bounded, revision-fenced access to the native
// Controller startup file.
package controllerconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"unicode/utf8"

	commonconfig "github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	maximumDocumentBytes = 1 << 20
	maximumReplayBytes   = 8 << 20
	maximumReplayRecords = 4096
	replayVersion        = 1
)

var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{16,128}$`)

type Store struct {
	mu              sync.Mutex
	path            string
	startupRevision string
}

type replayState string

const (
	replayPending   replayState = "pending"
	replayCompleted replayState = "completed"
)

type replayResponse struct {
	Path            string `json:"path"`
	Content         string `json:"content"`
	Revision        string `json:"revision"`
	RestartRequired bool   `json:"restart_required"`
}

type replayRecord struct {
	Version          int            `json:"version"`
	Key              string         `json:"key"`
	IntentDigest     string         `json:"intent_digest"`
	ExpectedRevision string         `json:"expected_revision"`
	TargetRevision   string         `json:"target_revision"`
	State            replayState    `json:"state"`
	Response         replayResponse `json:"response"`
}

func New(ctx context.Context, path string, startupDocument []byte) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) {
		return nil, errs.New(errs.KindInternal, "controller config path must be absolute")
	}
	currentDocument, err := readDocument(path)
	if err != nil {
		return nil, err
	}
	store := &Store{path: path, startupRevision: revision(startupDocument)}
	if err := store.recoverPending(ctx, currentDocument); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Path() string { return s.path }

func (s *Store) Current(ctx context.Context) (string, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", "", false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	content, err := readDocument(s.path)
	if err != nil {
		return "", "", false, err
	}
	currentRevision := revision(content)
	return string(content), currentRevision, currentRevision != s.startupRevision, nil
}

func (s *Store) Replace(
	ctx context.Context,
	idempotencyKey string,
	expectedRevision string,
	content string,
) (string, string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", "", false, err
	}
	if !idempotencyKeyPattern.MatchString(idempotencyKey) {
		return "", "", false, errs.New(errs.KindValidationFailed, "idempotency key is invalid")
	}
	bytesToWrite := []byte(content)
	if len(bytesToWrite) > maximumDocumentBytes {
		return "", "", false, errs.New(errs.KindValidationFailed, "controller config exceeds 1 MiB")
	}
	if !utf8.Valid(bytesToWrite) || bytes.IndexByte(bytesToWrite, 0) >= 0 {
		return "", "", false, errs.New(
			errs.KindValidationFailed,
			"controller config must contain valid NUL-free UTF-8",
		)
	}
	if _, err := commonconfig.ParseControllerDocument(ctx, bytesToWrite); err != nil {
		return "", "", false, errs.New(errs.KindValidationFailed, err.Error())
	}

	intentDigest := replacementIntentDigest(expectedRevision, bytesToWrite)
	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := readDocument(s.path)
	if err != nil {
		return "", "", false, err
	}
	if err := s.recoverPending(ctx, current); err != nil {
		return "", "", false, err
	}
	existing, found, err := s.readReplay(idempotencyKey)
	if err != nil {
		return "", "", false, err
	}
	if found {
		if subtle.ConstantTimeCompare([]byte(existing.IntentDigest), []byte(intentDigest)) != 1 {
			return "", "", false, errs.New(
				errs.KindIdempotencyMismatch,
				"idempotency key was used for a different Controller config replacement",
			)
		}
		if existing.State != replayCompleted {
			return "", "", false, errs.New(errs.KindStateConflict, "controller config replacement is unresolved")
		}
		return replayResult(existing.Response)
	}

	currentRevision := revision(current)
	if currentRevision != expectedRevision {
		return "", "", false, errs.New(
			errs.KindStateConflict,
			"controller config changed after it was loaded; reload before saving",
		)
	}
	requestedRevision := revision(bytesToWrite)
	response := replayResponse{
		Path:            s.path,
		Content:         content,
		Revision:        requestedRevision,
		RestartRequired: requestedRevision != s.startupRevision,
	}
	record := replayRecord{
		Version:          replayVersion,
		Key:              idempotencyKey,
		IntentDigest:     intentDigest,
		ExpectedRevision: expectedRevision,
		TargetRevision:   requestedRevision,
		State:            replayPending,
		Response:         response,
	}
	if err := s.writeReplay(record); err != nil {
		return "", "", false, err
	}
	if err := writeAtomicFile(s.path, bytesToWrite, 0o600); err != nil {
		return "", "", false, err
	}
	record.State = replayCompleted
	if err := s.writeReplay(record); err != nil {
		return "", "", false, err
	}
	return replayResult(response)
}

func replayResult(response replayResponse) (string, string, bool, error) {
	return response.Content, response.Revision, response.RestartRequired, nil
}

func (s *Store) recoverPending(ctx context.Context, current []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directoryPath := replayDirectoryPath(s.path)
	entries, err := os.ReadDir(directoryPath)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: read replay directory: %w", err))
	}
	if len(entries) > maximumReplayRecords {
		return errs.New(errs.KindInternal, "controller config replay record limit is exceeded")
	}
	currentRevision := revision(current)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		record, err := readReplayFile(filepath.Join(directoryPath, entry.Name()))
		if err != nil {
			return err
		}
		if err := validateReplayRecord(s.path, entry.Name(), record); err != nil {
			return err
		}
		if record.State != replayPending {
			continue
		}
		switch currentRevision {
		case record.TargetRevision:
			record.State = replayCompleted
			if err := s.writeReplay(record); err != nil {
				return err
			}
		case record.ExpectedRevision:
			if err := removeReplayFile(directoryPath, entry.Name()); err != nil {
				return err
			}
		default:
			return errs.New(
				errs.KindStateConflict,
				"controller config has an unresolved replacement; restore its expected or target revision",
			)
		}
	}
	return nil
}

func (s *Store) readReplay(key string) (replayRecord, bool, error) {
	path := replayFilePath(s.path, key)
	record, err := readReplayFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return replayRecord{}, false, nil
	}
	if err != nil {
		return replayRecord{}, false, err
	}
	if err := validateReplayRecord(s.path, filepath.Base(path), record); err != nil {
		return replayRecord{}, false, err
	}
	if record.Key != key {
		return replayRecord{}, false, errs.New(
			errs.KindIdempotencyMismatch,
			"idempotency key was used for a different Controller config replacement",
		)
	}
	return record, true, nil
}

func (s *Store) writeReplay(record replayRecord) error {
	if err := ensureReplayDirectory(replayDirectoryPath(s.path)); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: encode replay record: %w", err))
	}
	if len(encoded) > maximumReplayBytes {
		return errs.New(errs.KindInternal, "controller config replay record exceeds 8 MiB")
	}
	encoded = append(encoded, '\n')
	return writeAtomicFile(replayFilePath(s.path, record.Key), encoded, 0o600)
}

func validateReplayRecord(configPath string, filename string, record replayRecord) error {
	if record.Version != replayVersion ||
		!idempotencyKeyPattern.MatchString(record.Key) ||
		filename != replayFilename(record.Key) ||
		record.IntentDigest == "" ||
		record.ExpectedRevision == "" ||
		record.TargetRevision != revision([]byte(record.Response.Content)) ||
		record.Response.Path != configPath ||
		record.Response.Revision != record.TargetRevision ||
		(record.State != replayPending && record.State != replayCompleted) {
		return errs.New(errs.KindInternal, "controller config replay record is corrupt")
	}
	return nil
}

func readReplayFile(path string) (replayRecord, error) {
	content, err := readBoundedRegularFile(path, maximumReplayBytes)
	if err != nil {
		return replayRecord{}, err
	}
	var record replayRecord
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil {
		return replayRecord{}, errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("controller config: decode replay record: %w", err),
		)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return replayRecord{}, err
	}
	return record, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errs.New(errs.KindInternal, "controller config replay record has trailing data")
		}
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: decode replay record trailer: %w", err))
	}
	return nil
}

func ensureReplayDirectory(path string) error {
	created := false
	if err := os.Mkdir(path, 0o700); err != nil {
		if !errors.Is(err, fs.ErrExist) {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: create replay directory: %w", err))
		}
	} else {
		created = true
	}
	info, err := os.Lstat(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: inspect replay directory: %w", err))
	}
	if !info.IsDir() {
		return errs.New(errs.KindInternal, "controller config replay path is not a directory")
	}
	if info.Mode().Perm() != 0o700 {
		if err := os.Chmod(path, 0o700); err != nil {
			return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: set replay directory mode: %w", err))
		}
	}
	if created {
		return syncDirectory(filepath.Dir(path))
	}
	return nil
}

func removeReplayFile(directoryPath string, filename string) error {
	if err := os.Remove(filepath.Join(directoryPath, filename)); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: remove unapplied replay record: %w", err))
	}
	return syncDirectory(directoryPath)
}

func replayDirectoryPath(configPath string) string {
	return configPath + ".idempotency"
}

func replayFilePath(configPath string, key string) string {
	return filepath.Join(replayDirectoryPath(configPath), replayFilename(key))
}

func replayFilename(key string) string {
	digest := sha256.Sum256([]byte(key))
	return fmt.Sprintf("%x.json", digest)
}

func replacementIntentDigest(expectedRevision string, content []byte) string {
	digest := sha256.New()
	_, _ = io.WriteString(digest, "controller.config.set:v1\x00")
	_, _ = io.WriteString(digest, expectedRevision)
	_, _ = digest.Write([]byte{0})
	_, _ = digest.Write(content)
	return fmt.Sprintf("sha256:%x", digest.Sum(nil))
}

func readDocument(path string) ([]byte, error) {
	content, err := readBoundedRegularFile(path, maximumDocumentBytes)
	if errors.Is(err, fs.ErrNotExist) {
		return []byte{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(content) || bytes.IndexByte(content, 0) >= 0 {
		return nil, errs.New(errs.KindInternal, "controller config file must contain valid NUL-free UTF-8")
	}
	return content, nil
}

func readBoundedRegularFile(path string, maximumBytes int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: inspect file: %w", err))
		}
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errs.New(errs.KindInternal, "controller config path is not a regular file")
	}
	if info.Size() > maximumBytes {
		return nil, errs.New(errs.KindInternal, "controller config file exceeds its size limit")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: open file: %w", err))
	}
	content, readErr := io.ReadAll(io.LimitReader(file, maximumBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: read file: %w", readErr))
	}
	if closeErr != nil {
		return nil, errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: close file: %w", closeErr))
	}
	if int64(len(content)) > maximumBytes {
		return nil, errs.New(errs.KindInternal, "controller config file exceeds its size limit")
	}
	return content, nil
}

func writeAtomicFile(path string, content []byte, mode fs.FileMode) error {
	directoryPath := filepath.Dir(path)
	temporary, err := os.CreateTemp(directoryPath, ".groundplane-controller-config-")
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: create temporary file: %w", err))
	}
	temporaryPath := temporary.Name()
	keepTemporary := true
	defer func() {
		// Rationale: close and removal here are best-effort cleanup after the
		// authoritative operation error; replacing it would hide the failure
		// that determines whether publication can be retried safely.
		_ = temporary.Close()
		if keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(mode); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: set file mode: %w", err))
	}
	written, err := temporary.Write(content)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: write temporary file: %w", err))
	}
	if written != len(content) {
		return errs.Wrap(errs.KindInternal, io.ErrShortWrite)
	}
	if err := temporary.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: sync temporary file: %w", err))
	}
	if err := temporary.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: close temporary file: %w", err))
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: publish file: %w", err))
	}
	keepTemporary = false
	// The rename makes replacement atomic to readers. Syncing the containing
	// directory is the durability boundary for the new directory entry.
	return syncDirectory(directoryPath)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: open parent directory: %w", err))
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		if closeErr != nil {
			syncErr = errors.Join(syncErr, closeErr)
		}
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: sync parent directory: %w", syncErr))
	}
	if closeErr != nil {
		return errs.Wrap(errs.KindInternal, fmt.Errorf("controller config: close parent directory: %w", closeErr))
	}
	return nil
}

func revision(content []byte) string {
	digest := sha256.Sum256(content)
	return fmt.Sprintf("sha256:%x", digest)
}
