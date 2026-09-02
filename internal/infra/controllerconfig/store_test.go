package controllerconfig

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	commonconfig "github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/pkg/errs"
	"gopkg.in/yaml.v3"
)

// Rationale: Controller config replacement must preserve exact operator bytes,
// publish mode 0600, and replay the key-bound response after process restart.
func TestStoreReplacePersistsExactBytesAndDurableReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "controller.yaml")
	initial := validControllerDocument(t)
	if err := os.WriteFile(path, initial, 0o644); err != nil {
		t.Fatalf("write initial config: %v", err)
	}
	store, err := New(ctx, path, initial)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, initialRevision, restartRequired, err := store.Current(ctx)
	if err != nil || restartRequired {
		t.Fatalf("Current(initial) = %q/%t/%v", initialRevision, restartRequired, err)
	}

	updated := append([]byte("# retained update comment\n"), initial...)
	key := "controller-config-key-0001"
	content, updatedRevision, restartRequired, err := store.Replace(
		ctx, key, initialRevision, string(updated),
	)
	if err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	if content != string(updated) || updatedRevision != revision(updated) || !restartRequired {
		t.Fatalf("Replace() = %q/%q/%t", content, updatedRevision, restartRequired)
	}
	onDisk, err := os.ReadFile(path)
	if err != nil || string(onDisk) != string(updated) {
		t.Fatalf("config bytes = %q, %v", onDisk, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("config mode = %v, %v; want 0600", info.Mode().Perm(), err)
	}
	replayInfo, err := os.Stat(replayFilePath(path, key))
	if err != nil || replayInfo.Mode().Perm() != 0o600 {
		t.Fatalf("replay mode = %v, %v; want 0600", replayInfo.Mode().Perm(), err)
	}
	directoryInfo, err := os.Stat(replayDirectoryPath(path))
	if err != nil || directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("replay directory mode = %v, %v; want 0700", directoryInfo.Mode().Perm(), err)
	}

	restarted, err := New(ctx, path, updated)
	if err != nil {
		t.Fatalf("New(restart) error = %v", err)
	}
	replayedContent, replayedRevision, replayedRestart, err := restarted.Replace(
		ctx, key, initialRevision, string(updated),
	)
	if err != nil ||
		replayedContent != content ||
		replayedRevision != updatedRevision ||
		replayedRestart != restartRequired {
		t.Fatalf(
			"Replace(replay) = %q/%q/%t/%v, want %q/%q/%t",
			replayedContent,
			replayedRevision,
			replayedRestart,
			err,
			content,
			updatedRevision,
			restartRequired,
		)
	}

	_, _, _, err = restarted.Replace(ctx, key, updatedRevision, string(initial))
	if !errors.Is(err, errs.New(errs.KindIdempotencyMismatch, "")) {
		t.Fatalf("Replace(mismatch) error = %v, want idempotency mismatch", err)
	}
	_, _, _, err = restarted.Replace(ctx, "controller-config-key-0002", initialRevision, string(updated))
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("Replace(stale same content) error = %v, want state conflict", err)
	}
}

// Rationale: the config path may be atomically replaced after startup parsing
// but before store wiring. Restart-required must compare against the exact
// bytes parsed into the running process, never a second read of that path.
func TestStoreStartupRevisionUsesExactLoadedDocumentWhenPathChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "controller.yaml")
	initial := validControllerDocument(t)
	if err := os.WriteFile(path, initial, 0o600); err != nil {
		t.Fatalf("write initial config: %v", err)
	}
	_, startupDocument, err := commonconfig.LoadControllerDocument(ctx, path)
	if err != nil {
		t.Fatalf("LoadControllerDocument() error = %v", err)
	}

	changed := append([]byte("# replaced after startup parse\n"), initial...)
	if err := os.WriteFile(path, changed, 0o600); err != nil {
		t.Fatalf("replace config after startup parse: %v", err)
	}
	store, err := New(ctx, path, startupDocument)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	content, changedRevision, restartRequired, err := store.Current(ctx)
	if err != nil ||
		content != string(changed) ||
		changedRevision != revision(changed) ||
		!restartRequired {
		t.Fatalf(
			"Current() = %q/%q/%t/%v, want changed bytes and restart required",
			content,
			changedRevision,
			restartRequired,
			err,
		)
	}
	_, revertedRevision, restartRequired, err := store.Replace(
		ctx, "controller-config-key-race", changedRevision, string(startupDocument),
	)
	if err != nil || revertedRevision != revision(startupDocument) || restartRequired {
		t.Fatalf("Replace(revert) = %q/%t/%v, want parsed startup revision without restart", revertedRevision, restartRequired, err)
	}
}

// Rationale: restart-required is a byte comparison against the process startup
// snapshot, so an exact revert clears it without requiring a process restart.
func TestStoreRestartRequiredClearsOnExactRevert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "controller.yaml")
	initial := validControllerDocument(t)
	if err := os.WriteFile(path, initial, 0o600); err != nil {
		t.Fatalf("write initial config: %v", err)
	}
	store, err := New(ctx, path, initial)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, initialRevision, _, err := store.Current(ctx)
	if err != nil {
		t.Fatalf("Current() error = %v", err)
	}
	updated := append([]byte("# changed\n"), initial...)
	_, updatedRevision, restartRequired, err := store.Replace(
		ctx, "controller-config-key-0003", initialRevision, string(updated),
	)
	if err != nil || !restartRequired {
		t.Fatalf("Replace(update) restart/error = %t/%v", restartRequired, err)
	}
	content, revertedRevision, restartRequired, err := store.Replace(
		ctx, "controller-config-key-0004", updatedRevision, string(initial),
	)
	if err != nil ||
		content != string(initial) ||
		revertedRevision != initialRevision ||
		restartRequired {
		t.Fatalf("Replace(revert) = %q/%q/%t/%v", content, revertedRevision, restartRequired, err)
	}
}

// Rationale: the mutation boundary must reject oversized, NUL-bearing,
// non-UTF-8, and multi-document input before claiming an idempotency key.
func TestStoreReplaceUsesStrictBoundedStartupValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "controller.yaml")
	initial := validControllerDocument(t)
	if err := os.WriteFile(path, initial, 0o600); err != nil {
		t.Fatalf("write initial config: %v", err)
	}
	store, err := New(ctx, path, initial)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, initialRevision, _, err := store.Current(ctx)
	if err != nil {
		t.Fatalf("Current() error = %v", err)
	}
	tests := []struct {
		name    string
		content string
	}{
		{name: "over 1 MiB", content: strings.Repeat("#", maximumDocumentBytes+1)},
		{name: "NUL", content: string(initial) + "\x00"},
		{name: "invalid UTF-8", content: string([]byte{0xff})},
		{name: "multiple documents", content: string(initial) + "\n---\n{}\n"},
	}
	for index, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			key := fmt.Sprintf("controller-invalid-%04d", index)
			_, _, _, err := store.Replace(ctx, key, initialRevision, test.content)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Replace() error = %v, want validation failure", err)
			}
		})
	}
}

func validControllerDocument(t *testing.T) []byte {
	t.Helper()
	cfg := commonconfig.DefaultControllerConfig()
	cfg.EnvironmentPool = "10.0.0.0/9"
	cfg.SystemPool = "10.128.0.0/9"
	cfg.Runner.NetworkPool = "10.240.0.0/24"
	cfg.Runner.Image = "ghcr.io/example/runner@sha256:" + strings.Repeat("0", 64)
	cfg.Runner.HostUIDRange = "200000-200007"
	cfg.Runner.SubUIDRange = "300000-824287"
	cfg.Runner.SubGIDRange = "900000-1424287"
	content, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal Controller config: %v", err)
	}
	return append([]byte("# retained operator comment\n"), content...)
}
