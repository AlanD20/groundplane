package runnerjournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const (
	Schema                   = "groundplane.runner-journal/v1"
	maximumRevisionBytes     = 64 << 10
	maximumJournalRevisions  = 32768
	maximumOwnershipNonceLen = 64
	controllerExecutor       = "controller"
)

type revisionRecord struct {
	Schema   string                                 `json:"schema"`
	Progress runnerallocation.RunnerRuntimeProgress `json:"progress"`
}

type headRecord struct {
	Revision string `json:"revision"`
	Digest   string `json:"digest"`
}

// Journal persists Agent-owned write-before-host-effect state. The trusted
// root is constructor policy; no path from a persisted record is ever opened.
type Journal struct{ root string }

func New(root string) (*Journal, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, errs.New(errs.KindInternal, "runner journal root is invalid")
	}
	if err := ensureDirectory(root, 0o700); err != nil {
		return nil, err
	}
	return &Journal{root: root}, nil
}

func (journal *Journal) Begin(
	ctx context.Context,
	attempt runnerallocation.RunnerRuntimeAttempt,
	operation runnerallocation.RuntimeOperation,
) (runnerallocation.RunnerRuntimeProgress, error) {
	if err := validateAttempt(ctx, attempt, operation); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	directory, err := journal.operationDirectory(attempt)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	if err := ensureDirectory(directory, 0o700); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	current, exists, err := readLongestChain(directory)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	digest := attempt.Plan.Digest()
	if exists {
		if current.RunnerID != attempt.Plan.RunnerID || current.TaskID != attempt.TaskID ||
			current.Executor != attempt.Executor || current.Operation != operation ||
			current.PlanDigest != digest || current.RuntimeEpoch != attempt.Plan.RuntimeEpoch {
			return runnerallocation.RunnerRuntimeProgress{}, errs.New(
				errs.KindStateConflict,
				"runner journal operation identity changed",
			)
		}
		return current, nil
	}
	created := runnerallocation.RunnerRuntimeProgress{
		RunnerID: attempt.Plan.RunnerID, TaskID: attempt.TaskID, Executor: attempt.Executor,
		PlanDigest: digest, IdentityDigest: attempt.Plan.IdentityDigest(),
		RuntimeEpoch: attempt.Plan.RuntimeEpoch, Operation: operation,
		Status: runnerallocation.RuntimeStatusRunning, Revision: -1,
		OwnershipNonce: attempt.OwnershipNonce,
	}
	return appendRevision(directory, created)
}

func (journal *Journal) Issue(
	_ context.Context,
	current runnerallocation.RunnerRuntimeProgress,
	step runnerallocation.RunnerRuntimeStep,
) (runnerallocation.RunnerRuntimeProgress, error) {
	steps := operationSteps(current.Operation)
	if current.Status != runnerallocation.RuntimeStatusRunning || current.ActiveStep != nil ||
		current.NextStep >= len(steps) || steps[current.NextStep] != step {
		return runnerallocation.RunnerRuntimeProgress{}, errs.New(errs.KindStateConflict, "runner journal issue is invalid")
	}
	directory, err := journal.progressDirectory(current)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	current.ActiveStep = &step
	return appendRevision(directory, current)
}

func (journal *Journal) Checkpoint(
	_ context.Context,
	current runnerallocation.RunnerRuntimeProgress,
	evidence runnerallocation.RunnerRuntimeStepEvidence,
) (runnerallocation.RunnerRuntimeProgress, error) {
	steps := operationSteps(current.Operation)
	if current.Status != runnerallocation.RuntimeStatusRunning || current.ActiveStep == nil ||
		current.NextStep >= len(steps) || *current.ActiveStep != steps[current.NextStep] ||
		!evidence.Valid(current.Operation, *current.ActiveStep) {
		return runnerallocation.RunnerRuntimeProgress{}, errs.New(
			errs.KindStateConflict,
			"runner journal checkpoint is invalid",
		)
	}
	directory, err := journal.progressDirectory(current)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	if evidence.Ownership != nil {
		copy := *evidence.Ownership
		current.Evidence = &copy
	}
	if current.Operation == runnerallocation.RuntimeOperationRemove {
		current.CleanupReceipts = append(current.CleanupReceipts, evidence)
	}
	current.NextStep++
	current.ActiveStep = nil
	return appendRevision(directory, current)
}

func (journal *Journal) Fail(_ context.Context, current runnerallocation.RunnerRuntimeProgress) error {
	if current.Status != runnerallocation.RuntimeStatusRunning {
		return errs.New(errs.KindStateConflict, "runner journal lifecycle is not running")
	}
	directory, err := journal.progressDirectory(current)
	if err != nil {
		return err
	}
	current.Status = runnerallocation.RuntimeStatusFailed
	_, err = appendRevision(directory, current)
	return err
}

func (journal *Journal) Ready(
	_ context.Context,
	current runnerallocation.RunnerRuntimeProgress,
	evidence runnerallocation.RunnerRuntimeEvidence,
) error {
	if current.Operation != runnerallocation.RuntimeOperationCreate ||
		current.Status != runnerallocation.RuntimeStatusRunning || current.ActiveStep != nil ||
		current.NextStep != len(runnerallocation.CreationRuntimeSteps()) ||
		current.Evidence == nil || *current.Evidence != evidence || !evidence.Valid() {
		return errs.New(errs.KindStateConflict, "runner journal creation is not ready to seal")
	}
	directory, err := journal.progressDirectory(current)
	if err != nil {
		return err
	}
	current.Status = runnerallocation.RuntimeStatusReady
	_, err = appendRevision(directory, current)
	return err
}

func (journal *Journal) Removed(_ context.Context, current runnerallocation.RunnerRuntimeProgress) error {
	if current.Operation != runnerallocation.RuntimeOperationRemove ||
		current.Status != runnerallocation.RuntimeStatusRunning || current.ActiveStep != nil ||
		current.NextStep != len(runnerallocation.RemovalRuntimeSteps()) ||
		len(current.CleanupReceipts) != len(runnerallocation.RemovalRuntimeSteps()) {
		return errs.New(errs.KindStateConflict, "runner journal cleanup is not ready to seal")
	}
	directory, err := journal.progressDirectory(current)
	if err != nil {
		return err
	}
	current.Status = runnerallocation.RuntimeStatusRemoved
	_, err = appendRevision(directory, current)
	return err
}

func (journal *Journal) operationDirectory(attempt runnerallocation.RunnerRuntimeAttempt) (string, error) {
	return operationDirectory(
		journal.root,
		attempt.Plan.RunnerID,
		attempt.Plan.RuntimeEpoch,
		attempt.TaskID,
		attempt.OwnershipNonce,
	)
}

func (journal *Journal) progressDirectory(progress runnerallocation.RunnerRuntimeProgress) (string, error) {
	return operationDirectory(
		journal.root,
		progress.RunnerID,
		progress.RuntimeEpoch,
		progress.TaskID,
		progress.OwnershipNonce,
	)
}

func operationDirectory(root, runnerID string, epoch uint64, taskID, nonce string) (string, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil || ids.Validate(ids.KindTask, taskID) != nil ||
		epoch == 0 || !validLowerHex(nonce, maximumOwnershipNonceLen) {
		return "", errs.New(errs.KindInternal, "runner journal path identity is invalid")
	}
	return filepath.Join(root, "v1", runnerID, strconv.FormatUint(epoch, 10), taskID+"."+nonce), nil
}

func appendRevision(
	directory string,
	progress runnerallocation.RunnerRuntimeProgress,
) (runnerallocation.RunnerRuntimeProgress, error) {
	previousRevision := progress.Revision
	progress.Revision++
	if progress.Revision >= maximumJournalRevisions {
		return runnerallocation.RunnerRuntimeProgress{}, errs.New(errs.KindResourceInUse, "runner journal is exhausted")
	}
	if previousRevision < 0 {
		progress.PredecessorSHA256 = ""
	} else {
		previous, err := readBounded(filepath.Join(directory, revisionName(previousRevision)))
		if err != nil {
			return runnerallocation.RunnerRuntimeProgress{}, corruptJournal()
		}
		progress.PredecessorSHA256 = digestBytes(previous)
	}
	value, err := encodeRevision(progress)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	defer clear(value)
	name := filepath.Join(directory, revisionName(progress.Revision))
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindStateConflict, err)
	}
	writeErr := writeSyncClose(file, value)
	if writeErr != nil {
		return runnerallocation.RunnerRuntimeProgress{}, writeErr
	}
	if err := syncDirectory(directory); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	head, err := json.Marshal(headRecord{
		Revision: strconv.FormatInt(progress.Revision, 10), Digest: digestBytes(value),
	})
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(head)
	temporary := filepath.Join(directory, fmt.Sprintf(".HEAD.%d.%020d", os.Getpid(), progress.Revision))
	headFile, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := writeSyncClose(headFile, head); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	if err := os.Rename(temporary, filepath.Join(directory, "HEAD")); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := syncDirectory(directory); err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, err
	}
	return progress, nil
}

func readLongestChain(directory string) (runnerallocation.RunnerRuntimeProgress, bool, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return runnerallocation.RunnerRuntimeProgress{}, false, errs.Wrap(errs.KindInternal, err)
	}
	revisions := map[int64][]byte{}
	maximum := int64(-1)
	for _, entry := range entries {
		name := entry.Name()
		if name == "HEAD" || strings.HasPrefix(name, ".HEAD.") {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 || entry.IsDir() || len(name) != 25 || !strings.HasSuffix(name, ".json") {
			return runnerallocation.RunnerRuntimeProgress{}, false, corruptJournal()
		}
		revision, parseErr := strconv.ParseInt(strings.TrimSuffix(name, ".json"), 10, 64)
		if parseErr != nil || revision < 0 || revision >= maximumJournalRevisions || name != revisionName(revision) {
			return runnerallocation.RunnerRuntimeProgress{}, false, corruptJournal()
		}
		value, readErr := readBounded(filepath.Join(directory, name))
		if readErr != nil {
			return runnerallocation.RunnerRuntimeProgress{}, false, corruptJournal()
		}
		revisions[revision] = value
		if revision > maximum {
			maximum = revision
		}
	}
	if maximum < 0 {
		return runnerallocation.RunnerRuntimeProgress{}, false, nil
	}
	wantPredecessor := ""
	var current runnerallocation.RunnerRuntimeProgress
	for revision := int64(0); revision <= maximum; revision++ {
		value, ok := revisions[revision]
		if !ok {
			return runnerallocation.RunnerRuntimeProgress{}, false, corruptJournal()
		}
		progress, decodeErr := decodeRevision(value)
		if decodeErr != nil || progress.Revision != revision || progress.PredecessorSHA256 != wantPredecessor {
			return runnerallocation.RunnerRuntimeProgress{}, false, corruptJournal()
		}
		wantPredecessor = digestBytes(value)
		current = progress
	}
	return current, true, nil
}

func encodeRevision(progress runnerallocation.RunnerRuntimeProgress) ([]byte, error) {
	if err := validateProgress(progress); err != nil {
		return nil, err
	}
	value, err := json.Marshal(revisionRecord{Schema: Schema, Progress: progress})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(value) > maximumRevisionBytes {
		clear(value)
		return nil, errs.New(errs.KindResourceInUse, "runner journal revision is too large")
	}
	return value, nil
}

func decodeRevision(value []byte) (runnerallocation.RunnerRuntimeProgress, error) {
	if len(value) == 0 || len(value) > maximumRevisionBytes || rejectDuplicateJSONFields(value) != nil {
		return runnerallocation.RunnerRuntimeProgress{}, corruptJournal()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	var record revisionRecord
	if decoder.Decode(&record) != nil || requireJSONEOF(decoder) != nil || record.Schema != Schema ||
		validateProgress(record.Progress) != nil {
		return runnerallocation.RunnerRuntimeProgress{}, corruptJournal()
	}
	return record.Progress, nil
}

func validateAttempt(
	ctx context.Context,
	attempt runnerallocation.RunnerRuntimeAttempt,
	operation runnerallocation.RuntimeOperation,
) error {
	if ctx == nil || ctx.Err() != nil || ids.Validate(ids.KindTask, attempt.TaskID) != nil ||
		attempt.Executor != controllerExecutor || ids.Validate(ids.KindRunner, attempt.Plan.RunnerID) != nil ||
		attempt.Plan.RuntimeEpoch == 0 || attempt.Plan.Digest() == "" ||
		!validLowerHex(attempt.OwnershipNonce, maximumOwnershipNonceLen) ||
		(operation != runnerallocation.RuntimeOperationCreate && operation != runnerallocation.RuntimeOperationRemove) {
		return errs.New(errs.KindValidationFailed, "runner journal attempt is invalid")
	}
	return nil
}

func validateProgress(progress runnerallocation.RunnerRuntimeProgress) error {
	if ids.Validate(ids.KindRunner, progress.RunnerID) != nil || ids.Validate(ids.KindTask, progress.TaskID) != nil ||
		progress.Executor != controllerExecutor || !validDigest(progress.PlanDigest) ||
		!validDigest(progress.IdentityDigest) || progress.RuntimeEpoch == 0 ||
		!validLowerHex(progress.OwnershipNonce, maximumOwnershipNonceLen) || progress.Revision < 0 ||
		progress.Revision >= maximumJournalRevisions ||
		(progress.Revision == 0) != (progress.PredecessorSHA256 == "") ||
		(progress.Revision > 0 && !validDigest(progress.PredecessorSHA256)) {
		return corruptJournal()
	}
	steps := operationSteps(progress.Operation)
	if len(steps) == 0 || progress.NextStep < 0 || progress.NextStep > len(steps) {
		return corruptJournal()
	}
	if progress.ActiveStep != nil &&
		(progress.NextStep >= len(steps) || *progress.ActiveStep != steps[progress.NextStep]) {
		return corruptJournal()
	}
	return nil
}

func ensureDirectory(path string, mode os.FileMode) error {
	volume := filepath.VolumeName(path)
	current := volume + string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(path, current), string(filepath.Separator)) {
		if component == "" {
			continue
		}
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := os.Mkdir(current, mode); err != nil && !errors.Is(err, os.ErrExist) {
				return errs.Wrap(errs.KindInternal, err)
			}
			info, err = os.Lstat(current)
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errs.New(errs.KindInternal, "runner journal directory is unsafe")
		}
	}
	return nil
}

func readBounded(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, maximumRevisionBytes+1))
	if err != nil || len(value) > maximumRevisionBytes {
		clear(value)
		return nil, corruptJournal()
	}
	return value, nil
}

func writeSyncClose(file *os.File, value []byte) error {
	if _, err := file.Write(value); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := file.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func operationSteps(operation runnerallocation.RuntimeOperation) []runnerallocation.RunnerRuntimeStep {
	if operation == runnerallocation.RuntimeOperationCreate {
		return runnerallocation.CreationRuntimeSteps()
	}
	if operation == runnerallocation.RuntimeOperationRemove {
		return runnerallocation.RemovalRuntimeSteps()
	}
	return nil
}

func revisionName(revision int64) string { return fmt.Sprintf("%020d.json", revision) }

func digestBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validDigest(value string) bool {
	return strings.HasPrefix(value, "sha256:") && validLowerHex(strings.TrimPrefix(value, "sha256:"), 64)
}

func validLowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func rejectDuplicateJSONFields(value []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(value))
	if err := walkJSONValue(decoder); err != nil {
		return err
	}
	return requireJSONEOF(decoder)
}

func walkJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, tokenErr := decoder.Token()
			if tokenErr != nil {
				return tokenErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("JSON object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate JSON field %q", key)
			}
			seen[key] = struct{}{}
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
	case '[':
		for decoder.More() {
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	return err
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func corruptJournal() error { return errs.New(errs.KindInternal, "runner journal is corrupt") }
