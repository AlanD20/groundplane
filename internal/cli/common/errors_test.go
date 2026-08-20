package common

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: CLI rendering must project canonical Kind-owned fields and ignore
// mutations to the embedded framework schema carrier.
func TestHandleErrorsUsesCanonicalProblemProjection(t *testing.T) {
	oldStderr := os.Stderr
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stderr pipe: %v", err)
	}
	os.Stderr = writeEnd
	t.Cleanup(func() {
		os.Stderr = oldStderr
		_ = readEnd.Close()
		_ = writeEnd.Close()
	})

	domainError := errs.New(errs.KindStorageUnavailable, "storage is offline")
	domainError.Problem = errs.Problem{
		Code:   errs.Code("mutated.code"),
		Status: 200,
		Detail: "mutated detail",
	}
	exitCode := HandleErrors(domainError)
	if err := writeEnd.Close(); err != nil {
		t.Fatalf("close stderr writer: %v", err)
	}
	output, err := io.ReadAll(readEnd)
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	if exitCode != 1 || !strings.Contains(string(output), "storage.unavailable: storage is offline") {
		t.Fatalf("exit = %d, stderr = %q", exitCode, output)
	}
	if strings.Contains(string(output), "mutated") {
		t.Fatalf("CLI rendered mutable problem fields: %q", output)
	}
}

// Rationale: secret-bearing diagnostics passed to an opaque 500 constructor
// must remain in Error() for logs but never enter operator-facing CLI output.
func TestHandleErrorsSanitizesNewInternalMessage(t *testing.T) {
	oldStderr := os.Stderr
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stderr pipe: %v", err)
	}
	os.Stderr = writeEnd
	t.Cleanup(func() {
		os.Stderr = oldStderr
		_ = readEnd.Close()
		_ = writeEnd.Close()
	})

	const secret = "registry-token=private"
	domainError := errs.New(errs.KindInternal, secret, errs.WithTitle(secret))
	if !strings.Contains(domainError.Error(), secret) {
		t.Fatalf("Error() lost private diagnostics: %q", domainError.Error())
	}
	exitCode := HandleErrors(domainError)
	if err := writeEnd.Close(); err != nil {
		t.Fatalf("close stderr writer: %v", err)
	}
	output, err := io.ReadAll(readEnd)
	if err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	if exitCode != 1 || !strings.Contains(string(output), "internal: Internal Server Error") {
		t.Fatalf("exit = %d, stderr = %q", exitCode, output)
	}
	if strings.Contains(string(output), secret) {
		t.Fatalf("CLI leaked private diagnostics: %q", output)
	}
}
