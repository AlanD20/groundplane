package agentcredential

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: restart recovery must recreate both exact runtime files from
// durable ciphertext without rotating the stable Agent credential.
func TestMaterializeRecreatesExactRuntimeFromStoredCiphertext(t *testing.T) {
	t.Parallel()

	raw := bytes.Repeat([]byte{0x5a}, agentprotocol.RawTokenBytes)
	opener := &capturingOpener{plaintext: raw}
	manager := testManager(t, bytes.NewReader(nil), &capturingSealer{}, opener)
	runtimeConfig := testRuntimeConfig()

	materialize := func() {
		t.Helper()
		if err := manager.Materialize(
			context.Background(),
			testAgentID,
			EncryptedToken{Ciphertext: []byte("durable-ciphertext")},
			runtimeConfig,
		); err != nil {
			t.Fatalf("Materialize() error = %v", err)
		}
	}
	materialize()

	directory := manager.testPath("run/groundplane/agents/" + testAgentID)
	tokenPath := filepath.Join(directory, "token")
	configPath := filepath.Join(directory, "config.yaml")
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	wantToken := base64.RawURLEncoding.EncodeToString(raw)
	if string(token) != wantToken || len(token) != agentprotocol.EncodedTokenBytes {
		t.Fatalf("token = %q, want exact unpadded base64url %q", token, wantToken)
	}
	contents, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	wantConfig := "agent_id: agt_01ARZ3NDEKTSV4RRFFQ69G5FAV\n" +
		"log:\n" +
		"    level: warn\n" +
		"    console:\n" +
		"        enabled: true\n" +
		"    file:\n" +
		"        enabled: true\n" +
		"        path: /var/log/groundplane/agent.log\n" +
		"runtime:\n" +
		"    pull_interval_seconds: 2\n" +
		"    max_concurrent_tasks: 3\n" +
		"    labels:\n" +
		"        arch: arm64\n" +
		"        role: local\n"
	if string(contents) != wantConfig {
		t.Fatalf("config =\n%s\nwant exact canonical config =\n%s", contents, wantConfig)
	}
	assertMetadata(t, directory, 0o700, uint32(os.Geteuid()))
	assertMetadata(t, tokenPath, 0o400, uint32(os.Geteuid()))
	assertMetadata(t, configPath, 0o444, uint32(os.Geteuid()))
	if !allZero(opener.returned) {
		t.Fatal("Materialize() did not clear opener-owned plaintext buffer")
	}

	runtimeConfig.Runtime.MaxConcurrentTasks = 5
	opener.plaintext = raw
	materialize()
	restartedToken, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read restarted token: %v", err)
	}
	if !bytes.Equal(restartedToken, token) {
		t.Fatal("restart materialization rotated the stable token")
	}
	if opener.calls != 2 {
		t.Fatalf("opener calls = %d, want one per materialization", opener.calls)
	}
}

// Rationale: malformed or failed decryption must happen before runtime paths
// are created and must not disclose untrusted plaintext through the error.
func TestMaterializeFailsClosedBeforeFilesystemMutation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		opener *capturingOpener
	}{
		{
			name: "opener failure",
			opener: &capturingOpener{
				plaintext:         []byte("secret-from-opener"),
				failWithPlaintext: true,
			},
		},
		{name: "wrong plaintext length", opener: &capturingOpener{plaintext: []byte("short-secret")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			manager := testManager(t, bytes.NewReader(nil), &capturingSealer{}, test.opener)
			err := manager.Materialize(
				context.Background(),
				testAgentID,
				EncryptedToken{Ciphertext: []byte("ciphertext")},
				testRuntimeConfig(),
			)
			if err == nil {
				t.Fatal("Materialize() error = nil, want failure")
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("Materialize() error leaked plaintext: %v", err)
			}
			if _, statErr := os.Lstat(
				manager.testPath("run/groundplane/agents/" + testAgentID),
			); !errors.Is(
				statErr,
				os.ErrNotExist,
			) {
				t.Fatalf("runtime directory stat error = %v, want not exist", statErr)
			}
			if test.opener.returned != nil && !allZero(test.opener.returned) {
				t.Fatal("Materialize() did not clear malformed plaintext")
			}
		})
	}
}

// Rationale: materialization receives committed Controller state, so identity
// mismatch or invalid runtime policy is durable corruption rather than an
// operator validation failure.
func TestMaterializeClassifiesInvalidDurableStateAsInternal(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		mutate func(*config.AgentConfig)
	}{
		{name: "identity mismatch", mutate: func(runtimeConfig *config.AgentConfig) {
			runtimeConfig.AgentID = "agt_01BX5ZZKBKACTAV9WEVGEMMVRZ"
		}},
		{name: "invalid runtime policy", mutate: func(runtimeConfig *config.AgentConfig) {
			runtimeConfig.Runtime.MaxConcurrentTasks = 0
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			opener := &capturingOpener{plaintext: bytes.Repeat([]byte{0x2a}, agentprotocol.RawTokenBytes)}
			manager := testManager(t, bytes.NewReader(nil), &capturingSealer{}, opener)
			runtimeConfig := testRuntimeConfig()
			test.mutate(&runtimeConfig)
			err := manager.Materialize(
				context.Background(),
				testAgentID,
				EncryptedToken{Ciphertext: []byte("ciphertext")},
				runtimeConfig,
			)
			if err == nil {
				t.Fatal("Materialize() error = nil, want durable-state refusal")
			}
			var domainError *errs.Error
			if !errors.As(err, &domainError) || domainError.Code != errs.CodeInternal {
				t.Fatalf("Materialize() error = %v, want %q", err, errs.CodeInternal)
			}
			if opener.calls != 0 {
				t.Fatalf("opener calls = %d, want 0 before durable-state validation", opener.calls)
			}
			if _, statErr := os.Lstat(
				manager.testPath("run/groundplane/agents/" + testAgentID),
			); !errors.Is(
				statErr,
				os.ErrNotExist,
			) {
				t.Fatalf("runtime directory stat error = %v, want not exist", statErr)
			}
		})
	}
}

// Rationale: config must be durable before the token becomes visible, and a
// failed atomic replacement must leave no plaintext temporary file.
func TestMaterializePreservesFailureOrderingAndCleansTemporaryFiles(t *testing.T) {
	t.Parallel()

	manager := testManager(
		t,
		bytes.NewReader(nil),
		&capturingSealer{},
		&capturingOpener{plaintext: bytes.Repeat([]byte{0x31}, agentprotocol.RawTokenBytes)},
	)
	manager.rename = func(_ *os.Root, _, destination string) error {
		if destination == "config.yaml" {
			return errors.New("config rename blocked")
		}
		return errors.New("token must not be attempted")
	}

	err := manager.Materialize(
		context.Background(),
		testAgentID,
		EncryptedToken{Ciphertext: []byte("ciphertext")},
		testRuntimeConfig(),
	)
	if err == nil {
		t.Fatal("Materialize() error = nil, want config replacement failure")
	}
	directory := manager.testPath("run/groundplane/agents/" + testAgentID)
	if _, statErr := os.Lstat(filepath.Join(directory, "token")); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("token stat error = %v, want not exist", statErr)
	}
	for _, temporary := range []string{temporaryConfigName, temporaryTokenName} {
		if _, statErr := os.Lstat(filepath.Join(directory, temporary)); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("temporary %s stat error = %v, want not exist", temporary, statErr)
		}
	}
}

// Rationale: neither runtime target may redirect an atomic replacement through
// a symbolic link, even when the link target is otherwise writable.
func TestMaterializeRefusesRuntimeTargetSymlinks(t *testing.T) {
	t.Parallel()

	for _, targetName := range []string{"config.yaml", "token"} {
		t.Run(targetName, func(t *testing.T) {
			t.Parallel()
			manager := testManager(
				t,
				bytes.NewReader(nil),
				&capturingSealer{},
				&capturingOpener{plaintext: bytes.Repeat([]byte{0x33}, agentprotocol.RawTokenBytes)},
			)
			directory := manager.testPath("run/groundplane/agents/" + testAgentID)
			if err := os.MkdirAll(directory, 0o700); err != nil {
				t.Fatalf("create runtime directory: %v", err)
			}
			target := manager.testPath("outside")
			if err := os.WriteFile(target, []byte("untouched"), 0o600); err != nil {
				t.Fatalf("write symlink target: %v", err)
			}
			if err := os.Symlink(target, filepath.Join(directory, targetName)); err != nil {
				t.Fatalf("create target symlink: %v", err)
			}
			if err := manager.Materialize(
				context.Background(),
				testAgentID,
				EncryptedToken{Ciphertext: []byte("ciphertext")},
				testRuntimeConfig(),
			); err == nil {
				t.Fatal("Materialize() error = nil, want symlink refusal")
			}
			contents, err := os.ReadFile(target)
			if err != nil || string(contents) != "untouched" {
				t.Fatalf("symlink target = %q, error = %v; want untouched", contents, err)
			}
		})
	}
}

// Rationale: a wrong-owner trusted ancestor must be rejected before creating
// any Groundplane-managed runtime path.
func TestMaterializeValidatesParentOwnershipBeforeCreation(t *testing.T) {
	t.Parallel()

	hostRoot := t.TempDir()
	if err := os.Mkdir(filepath.Join(hostRoot, "run"), 0o755); err != nil {
		t.Fatalf("create run directory: %v", err)
	}
	manager, err := newManager(
		bytes.NewReader(nil),
		&capturingSealer{},
		&capturingOpener{plaintext: bytes.Repeat([]byte{0x55}, agentprotocol.RawTokenBytes)},
		hostRoot,
		uint32(os.Geteuid())^1,
	)
	if err != nil {
		t.Fatalf("newManager() error = %v", err)
	}
	if err := manager.Materialize(
		context.Background(),
		testAgentID,
		EncryptedToken{Ciphertext: []byte("ciphertext")},
		testRuntimeConfig(),
	); err == nil {
		t.Fatal("Materialize() error = nil, want wrong-owner refusal")
	}
	if _, err := os.Lstat(filepath.Join(hostRoot, "run", "groundplane")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("managed directory stat error = %v, want not exist", err)
	}
}

// Rationale: runtime reconciliation must serialize the fixed temporary names
// so concurrent retries cannot overwrite or consume one another's plaintext.
func TestMaterializeSerializesConcurrentReplacements(t *testing.T) {
	t.Parallel()

	manager := testManager(
		t,
		bytes.NewReader(nil),
		&capturingSealer{},
		&capturingOpener{plaintext: bytes.Repeat([]byte{0x61}, agentprotocol.RawTokenBytes)},
	)
	originalRename := manager.rename
	renameEntered := make(chan struct{})
	releaseRename := make(chan struct{})
	first := true
	manager.rename = func(root *os.Root, oldName, newName string) error {
		if first {
			first = false
			close(renameEntered)
			<-releaseRename
		}
		return originalRename(root, oldName, newName)
	}

	result := make(chan error, 2)
	for _, ciphertext := range []string{"first", "second"} {
		ciphertext := ciphertext
		go func() {
			result <- manager.Materialize(
				context.Background(),
				testAgentID,
				EncryptedToken{Ciphertext: []byte(ciphertext)},
				testRuntimeConfig(),
			)
		}()
		if ciphertext == "first" {
			<-renameEntered
			if manager.mu.TryLock() {
				manager.mu.Unlock()
				t.Fatal("Manager mutex was not held during runtime materialization")
			}
		}
	}
	close(releaseRename)
	for range 2 {
		if err := <-result; err != nil {
			t.Fatalf("concurrent Materialize() error = %v", err)
		}
	}
}

// Rationale: cleanup must remove only the canonical selected Agent runtime,
// tolerate repetition, and refuse a substituted directory.
func TestRemoveIsScopedIdempotentAndSymlinkSafe(t *testing.T) {
	t.Parallel()

	manager := testManager(
		t,
		bytes.NewReader(nil),
		&capturingSealer{},
		&capturingOpener{plaintext: bytes.Repeat([]byte{0x77}, agentprotocol.RawTokenBytes)},
	)
	if err := manager.Materialize(
		context.Background(),
		testAgentID,
		EncryptedToken{Ciphertext: []byte("ciphertext")},
		testRuntimeConfig(),
	); err != nil {
		t.Fatalf("Materialize() error = %v", err)
	}
	directory := manager.testPath("run/groundplane/agents/" + testAgentID)
	neighbor := manager.testPath("run/groundplane/agents/keep")
	if err := os.MkdirAll(neighbor, 0o700); err != nil {
		t.Fatalf("create neighbor: %v", err)
	}
	if err := manager.Remove(context.Background(), testAgentID); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := os.Lstat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime directory stat error = %v, want not exist", err)
	}
	if _, err := os.Stat(neighbor); err != nil {
		t.Fatalf("neighbor was removed: %v", err)
	}
	if err := manager.Remove(context.Background(), testAgentID); err != nil {
		t.Fatalf("idempotent Remove() error = %v", err)
	}

	target := manager.testPath("outside-runtime")
	if err := os.MkdirAll(target, 0o700); err != nil {
		t.Fatalf("create target: %v", err)
	}
	if err := os.WriteFile(filepath.Join(target, "keep"), []byte("keep"), 0o600); err != nil {
		t.Fatalf("write target marker: %v", err)
	}
	if err := os.Symlink(target, directory); err != nil {
		t.Fatalf("create runtime symlink: %v", err)
	}
	if err := manager.Remove(context.Background(), testAgentID); err == nil {
		t.Fatal("Remove() error = nil, want symlink refusal")
	}
	if _, err := os.Stat(filepath.Join(target, "keep")); err != nil {
		t.Fatalf("symlink target was modified: %v", err)
	}
}

func testManager(t *testing.T, random io.Reader, sealer Sealer, opener Opener) *Manager {
	t.Helper()
	manager, err := newManager(random, sealer, opener, t.TempDir(), uint32(os.Geteuid()))
	if err != nil {
		t.Fatalf("newManager() error = %v", err)
	}
	return manager
}

func testRuntimeConfig() config.AgentConfig {
	runtimeConfig := config.DefaultAgentConfig()
	runtimeConfig.AgentID = testAgentID
	runtimeConfig.Runtime.PullIntervalSeconds = 2
	runtimeConfig.Runtime.MaxConcurrentTasks = 3
	runtimeConfig.Runtime.Labels = map[string]string{"role": "local", "arch": "arm64"}
	return runtimeConfig
}

func (m *Manager) testPath(relative string) string {
	return filepath.Join(m.hostRoot, filepath.FromSlash(relative))
}

func assertMetadata(t *testing.T, name string, wantMode os.FileMode, wantUID uint32) {
	t.Helper()
	info, err := os.Stat(name)
	if err != nil {
		t.Fatalf("stat %s: %v", name, err)
	}
	if got := info.Mode().Perm(); got != wantMode {
		t.Fatalf("mode %s = %04o, want %04o", name, got, wantMode)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s has no ownership metadata", name)
	}
	if stat.Uid != wantUID {
		t.Fatalf("owner %s = %d, want %d", name, stat.Uid, wantUID)
	}
}

func allZero(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}
