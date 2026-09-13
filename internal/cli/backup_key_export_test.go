package cli

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// QA: BAK-14; local filesystem export only, not no-store HTTP headers or off-host recovery.
// Rationale: file export must preserve the exact private identity attachment
// while enforcing the contract's owner-only 0600 mode.
func TestWritePrivateExportFileWritesExactBytesWithPrivateMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.txt")
	body := []byte("AGE-SECRET-KEY-1TEST\n")
	if err := writePrivateExportFile(path, body); err != nil {
		t.Fatalf("writePrivateExportFile() error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("file = %q, want %q", got, body)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 600", info.Mode().Perm())
	}
}

// QA: BAK-14, UI-03; local filesystem refusal only, not server-side key handling.
// Rationale: O_NOFOLLOW must reject a symlink destination before truncating or
// writing the file it targets.
func TestWritePrivateExportFileRejectsSymlinkWithoutChangingTarget(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	link := filepath.Join(dir, "identity")
	if err := os.WriteFile(target, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := writePrivateExportFile(link, []byte("secret")); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("writePrivateExportFile(symlink) error = %v, want validation_failed", err)
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "unchanged" {
		t.Fatalf("symlink target changed to %q", got)
	}
}

// QA: BAK-14, UI-03; local CLI admission only, with no export request executed.
// Rationale: raw private-key export has its own --file contract and must reject
// the global structured-output formatter before making a request.
func TestBackupExportKeyRejectsGlobalOutput(t *testing.T) {
	command := newBackupCmd()
	command.PersistentFlags().String("output", "", "test global output")
	command.SetArgs([]string{"export-key", "--output", "json"})
	if err := command.Execute(); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Execute() error = %v, want validation_failed", err)
	}
}
