package runnerproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"
	"strings"
	"sync"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const maximumBuildReceiptBytes = 4 << 20
const maximumBuildInventory = 16384

// BuildIntent is durable before Docker receives any build bytes. It contains
// hashes, not build arguments, context contents or registry credentials. An
// interrupted build is never automatically submitted again.
type BuildIntent struct {
	OperationID   string   `json:"operation_id"`
	RunnerID      string   `json:"runner_id"`
	RuntimeEpoch  uint64   `json:"runtime_epoch"`
	DaemonID      string   `json:"daemon_id"`
	ContextDigest string   `json:"context_digest"`
	OptionsDigest string   `json:"options_digest"`
	BeforeImages  []string `json:"before_images"`
}

// BuildReceipt is the only successful result accepted for image delivery. A
// completed Docker stream alone is not proof of which image was produced.
type BuildReceipt struct {
	Intent       BuildIntent `json:"intent"`
	IntentDigest string      `json:"intent_digest"`
	ImageID      string      `json:"image_id"`
	NewImages    []string    `json:"new_images"`
	Succeeded    bool        `json:"succeeded"`
}

// BuildLedger keeps immutable issued/result files through an open directory
// descriptor. Its directory must be release-owned and unavailable to workflows.
// Results never overwrite intent, and a damaged or unresolved intent fences the
// daemon until the Controller explicitly reconciles its retained evidence.
type BuildLedger struct {
	root      *os.Root
	runnerID  string
	epoch     uint64
	mu        sync.Mutex
	directory *os.File
}

func OpenBuildLedger(ctx context.Context, directory, runnerID string, epoch uint64) (*BuildLedger, error) {
	if ctx == nil || ids.Validate(ids.KindRunner, runnerID) != nil || epoch == 0 {
		return nil, buildEvidenceConflict()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		return nil, buildEvidenceConflict()
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	directoryHandle, err := root.Open(".")
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, errors.Join(err, root.Close()))
	}
	opened, err := directoryHandle.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errs.Wrap(
			errs.KindInternal,
			errors.Join(buildEvidenceConflict(), directoryHandle.Close(), root.Close()),
		)
	}
	if err := unix.Flock(int(directoryHandle.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return nil, errs.Wrap(errs.KindStateConflict, errors.Join(err, directoryHandle.Close(), root.Close()))
	}
	return &BuildLedger{root: root, directory: directoryHandle, runnerID: runnerID, epoch: epoch}, nil
}

func (ledger *BuildLedger) Close() error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if err := errors.Join(ledger.root.Close(), ledger.directory.Close()); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func (ledger *BuildLedger) Issue(ctx context.Context, intent BuildIntent) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !validBuildIntent(intent) || intent.RunnerID != ledger.runnerID || intent.RuntimeEpoch != ledger.epoch {
		return buildEvidenceConflict()
	}
	if err := ledger.requireSettled(ctx); err != nil {
		return err
	}
	return ledger.publish(ctx, intent.OperationID+".issued.json", intent)
}

func (ledger *BuildLedger) Complete(ctx context.Context, receipt BuildReceipt) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !validBuildReceipt(receipt) || receipt.Intent.RunnerID != ledger.runnerID ||
		receipt.Intent.RuntimeEpoch != ledger.epoch {
		return buildEvidenceConflict()
	}
	issued, err := readBuildRecord[BuildIntent](ledger.root, receipt.Intent.OperationID+".issued.json")
	if err != nil || buildRecordDigest(issued) != receipt.IntentDigest {
		return buildEvidenceConflict()
	}
	return ledger.publish(ctx, receipt.Intent.OperationID+".result.json", receipt)
}

func (ledger *BuildLedger) Receipt(ctx context.Context, operationID string) (BuildReceipt, error) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ctx == nil || ids.Validate(ids.KindOperation, operationID) != nil {
		return BuildReceipt{}, buildEvidenceConflict()
	}
	if err := ctx.Err(); err != nil {
		return BuildReceipt{}, err
	}
	return ledger.receipt(operationID)
}

func (ledger *BuildLedger) receipt(operationID string) (BuildReceipt, error) {
	intent, err := readBuildRecord[BuildIntent](ledger.root, operationID+".issued.json")
	if err != nil {
		return BuildReceipt{}, err
	}
	receipt, err := readBuildRecord[BuildReceipt](ledger.root, operationID+".result.json")
	if err != nil || !validBuildReceipt(receipt) || intent.OperationID != operationID ||
		intent.RunnerID != ledger.runnerID || intent.RuntimeEpoch != ledger.epoch ||
		receipt.IntentDigest != buildRecordDigest(intent) {
		return BuildReceipt{}, buildEvidenceConflict()
	}
	return receipt, nil
}

func (ledger *BuildLedger) requireSettled(ctx context.Context) error {
	if ctx == nil {
		return buildEvidenceConflict()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	directory, err := ledger.root.Open(".")
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer directory.Close()
	for {
		entries, readErr := directory.ReadDir(128)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			name := entry.Name()
			if strings.HasPrefix(name, ".staging-") {
				continue
			}
			operationID, known := strings.CutSuffix(name, ".issued.json")
			if !known {
				operationID, known = strings.CutSuffix(name, ".result.json")
			}
			if !known || ids.Validate(ids.KindOperation, operationID) != nil || !entry.Type().IsRegular() {
				return buildEvidenceConflict()
			}
			if _, err := ledger.receipt(operationID); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return errs.Wrap(errs.KindInternal, readErr)
		}
	}
}

func (ledger *BuildLedger) publish(ctx context.Context, name string, value buildRecord) error {
	if ctx == nil {
		return buildEvidenceConflict()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	canonical, err := jcs.Canonicalize(encoded)
	if err != nil || len(canonical) > maximumBuildReceiptBytes {
		return buildEvidenceConflict()
	}
	defer clear(canonical)
	stage := ".staging-" + ids.NewULID()
	file, err := ledger.root.OpenFile(stage, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer ledger.root.Remove(stage) // Unpublished private scratch never grants build authority.
	_, writeErr := file.Write(canonical)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ledger.root.Link(stage, name); err != nil {
		if errors.Is(err, os.ErrExist) {
			return buildEvidenceConflict()
		}
		return errs.Wrap(errs.KindInternal, err)
	}
	if err := ledger.directory.Sync(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

// Only these closed records may be persisted by this ledger.
type buildRecord interface{ buildRecord() }

func (BuildIntent) buildRecord()  {}
func (BuildReceipt) buildRecord() {}

func readBuildRecord[T BuildIntent | BuildReceipt](root *os.Root, name string) (T, error) {
	var zero T
	file, err := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return zero, buildEvidenceConflict()
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || info.Size() > maximumBuildReceiptBytes {
		return zero, buildEvidenceConflict()
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maximumBuildReceiptBytes+1))
	if err != nil || len(encoded) > maximumBuildReceiptBytes {
		return zero, buildEvidenceConflict()
	}
	defer clear(encoded)
	value, err := jcs.Decode[T](encoded)
	if err != nil {
		return zero, buildEvidenceConflict()
	}
	return value, nil
}

func buildRecordDigest(value buildRecord) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	defer clear(encoded)
	canonical, err := jcs.Canonicalize(encoded)
	if err != nil {
		return ""
	}
	defer clear(canonical)
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func validBuildIntent(intent BuildIntent) bool {
	return ids.Validate(ids.KindOperation, intent.OperationID) == nil &&
		ids.Validate(ids.KindRunner, intent.RunnerID) == nil &&
		intent.RuntimeEpoch > 0 &&
		intent.DaemonID != "" &&
		len(intent.DaemonID) <= 128 &&
		validImageDigest(intent.ContextDigest) &&
		validImageDigest(intent.OptionsDigest) &&
		validImageInventory(intent.BeforeImages)
}

func validBuildReceipt(receipt BuildReceipt) bool {
	if !validBuildIntent(receipt.Intent) || receipt.IntentDigest != buildRecordDigest(receipt.Intent) ||
		!validImageInventory(receipt.NewImages) {
		return false
	}
	if receipt.Succeeded {
		if !validImageDigest(receipt.ImageID) || !slices.Contains(receipt.NewImages, receipt.ImageID) {
			return false
		}
	} else if receipt.ImageID != "" {
		return false
	}
	for _, imageID := range receipt.NewImages {
		if _, found := slices.BinarySearch(receipt.Intent.BeforeImages, imageID); found {
			return false
		}
	}
	return true
}

func validImageInventory(images []string) bool {
	if images == nil || len(images) > maximumBuildInventory || !slices.IsSorted(images) {
		return false
	}
	for index, imageID := range images {
		if !validImageDigest(imageID) || (index > 0 && images[index-1] == imageID) {
			return false
		}
	}
	return true
}

func validImageDigest(value string) bool {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[7:] {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func buildEvidenceConflict() error {
	return errs.New(errs.KindStateConflict, "Runner build evidence is unresolved or inconsistent")
}
