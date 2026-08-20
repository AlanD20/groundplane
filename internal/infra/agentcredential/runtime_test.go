package agentcredential

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Rationale: cleanup must remove only the selected Agent runtime and tolerate repetition.
func TestDeleteRuntimeRemovesOnlyCanonicalAgentDirectoryAndIsIdempotent(t *testing.T) {
	t.Parallel()

	manager := testManager(t, bytes.NewReader(bytes.Repeat([]byte{0x77}, tokenBytes)), &capturingSealer{ciphertext: []byte("sealed")})
	if _, err := manager.GenerateAndMaterialize(context.Background(), testAgentID); err != nil {
		t.Fatalf("materialize: %v", err)
	}
	directory := manager.testPath("run/groundplane/agents/" + testAgentID)
	neighbor := manager.testPath("run/groundplane/agents/keep")
	if err := os.MkdirAll(neighbor, 0o700); err != nil {
		t.Fatalf("create neighbor: %v", err)
	}
	if err := manager.DeleteRuntime(context.Background(), testAgentID); err != nil {
		t.Fatalf("DeleteRuntime() error = %v", err)
	}
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime directory stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(neighbor); err != nil {
		t.Fatalf("neighbor was removed: %v", err)
	}
	if err := manager.DeleteRuntime(context.Background(), testAgentID); err != nil {
		t.Fatalf("idempotent DeleteRuntime() error = %v", err)
	}
}

// Rationale: validated identifiers and real directories prevent cleanup path substitution.
func TestDeleteRuntimeRefusesInvalidIDAndDirectorySymlink(t *testing.T) {
	t.Parallel()

	manager := testManager(t, bytes.NewReader(bytes.Repeat([]byte{0x88}, tokenBytes)), &capturingSealer{ciphertext: []byte("sealed")})
	if err := manager.DeleteRuntime(context.Background(), "agt_../../target"); err == nil {
		t.Fatal("DeleteRuntime() error = nil, want invalid ID rejection")
	}

	target := manager.testPath("target")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "keep"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write target marker: %v", err)
	}
	link := manager.testPath("run/groundplane/agents/" + testAgentID)
	if err := os.MkdirAll(filepath.Dir(link), 0o700); err != nil {
		t.Fatalf("create link parent: %v", err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create runtime symlink: %v", err)
	}
	if err := manager.DeleteRuntime(context.Background(), testAgentID); err == nil {
		t.Fatal("DeleteRuntime() error = nil, want symlink refusal")
	}
	if _, err := os.Stat(filepath.Join(target, "keep")); err != nil {
		t.Fatalf("symlink target was modified: %v", err)
	}
}

// Rationale: a wrong-owner trusted parent must be rejected before creating credential paths.
func TestMaterializeValidatesParentOwnershipBeforeCreatingManagedDirectories(t *testing.T) {
	t.Parallel()

	hostRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(hostRoot, "run"), 0o755); err != nil {
		t.Fatalf("create run directory: %v", err)
	}
	wrongUID := uint32(os.Geteuid()) ^ 1
	manager, err := newManager(bytes.NewReader(nil), &capturingSealer{}, hostRoot, wrongUID)
	if err != nil {
		t.Fatalf("newManager() error = %v", err)
	}
	if err := manager.materializeToken(context.Background(), testAgentID, []byte("not-logged")); err == nil {
		t.Fatal("materializeToken() error = nil, want wrong-owner refusal")
	}
	if _, err := os.Lstat(filepath.Join(hostRoot, "run", "groundplane")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed directory stat error = %v, want not exist", err)
	}
}

// Rationale: the materializer must harden managed directories without mutating trusted /run.
func TestMaterializePreservesRunModeAndEnforcesManagedDirectoryModes(t *testing.T) {
	t.Parallel()

	manager := testManager(t, bytes.NewReader(nil), &capturingSealer{})
	run := manager.testPath("run")
	agent := manager.testPath("run/groundplane/agents/" + testAgentID)
	if err := os.MkdirAll(agent, 0o755); err != nil {
		t.Fatalf("create runtime hierarchy: %v", err)
	}
	groundplane := filepath.Dir(filepath.Dir(agent))
	agents := filepath.Dir(agent)
	for _, directory := range []string{run, groundplane, agents, agent} {
		if err := os.Chmod(directory, 0o755); err != nil {
			t.Fatalf("set directory mode: %v", err)
		}
	}

	if err := manager.materializeToken(context.Background(), testAgentID, []byte("token")); err != nil {
		t.Fatalf("materializeToken() error = %v", err)
	}
	assertMode(t, run, 0o755)
	assertMode(t, groundplane, 0o700)
	assertMode(t, agents, 0o700)
	assertMode(t, agent, 0o700)
}

func testManager(t *testing.T, random io.Reader, sealer Sealer) *Manager {
	t.Helper()
	manager, err := newManager(random, sealer, t.TempDir(), uint32(os.Geteuid()))
	if err != nil {
		t.Fatalf("newManager() error = %v", err)
	}
	return manager
}

func (m *Manager) testPath(relative string) string {
	return filepath.Join(m.hostRoot, filepath.FromSlash(relative))
}

func assertMode(t *testing.T, name string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(name)
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("mode %s = %04o, want %04o", name, got, want)
	}
}
