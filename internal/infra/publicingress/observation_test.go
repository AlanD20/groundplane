package publicingress

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestObservationRequiresExactSettledAndReadyRuntime(t *testing.T) {
	current := []byte(`{"routes":["current"]}`)
	next := []byte(`{"routes":["candidate"]}`)
	expected := Receipt{
		RuntimeID: strings.Repeat("1", 64), Revision: 3, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(current)),
	}
	candidate := Receipt{
		RuntimeID: expected.RuntimeID,
		Revision:  4,
		Digest:    fmt.Sprintf("sha256:%x", sha256.Sum256(next)),
	}
	for _, scenario := range []struct {
		name              string
		stage, marker     bool
		unrecorded        bool
		privatePathFailed bool
		loaded            []byte
		want              Receipt
		active, ready     bool
		success           bool
	}{
		{name: "exact active", loaded: current, want: expected, active: true, ready: true, success: true},
		{name: "different revision with same bytes", loaded: current, want: Receipt{
			RuntimeID: expected.RuntimeID, Revision: candidate.Revision, Digest: expected.Digest,
		}},
		{name: "different runtime with same bytes", loaded: current, want: Receipt{
			RuntimeID: strings.Repeat("2", 64), Revision: expected.Revision, Digest: expected.Digest,
		}},
		{name: "unknown active bytes", loaded: next, want: expected, active: true},
		{name: "private path failed", loaded: current, want: expected, active: true, ready: true, privatePathFailed: true},
		{name: "candidate staged only", loaded: current, want: candidate, stage: true},
		{name: "candidate loaded without settlement", loaded: next, want: candidate, stage: true},
		{name: "unresolved reload", loaded: current, want: expected, marker: true},
		{name: "startup exists without ownership", loaded: current, want: expected, unrecorded: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := t.TempDir()
			if err := os.Chmod(path, 0o700); err != nil {
				t.Fatal(err)
			}
			runtime := &observationRuntime{t: t, expected: expected, loaded: scenario.loaded}
			if scenario.privatePathFailed {
				runtime.err = conflict("test private relay unavailable")
			}
			store, err := New(path, runtime)
			if err != nil {
				t.Fatal(err)
			}
			writeObservationFixture(t, store, current, next, expected, candidate, scenario.unrecorded, scenario.stage)
			if scenario.marker {
				if err := os.WriteFile(filepath.Join(path, reloadMarker), []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			before := observationFiles(t, path)
			receipt, err := store.Observe(t.Context(), scenario.want)
			if (err == nil) != scenario.success || err == nil && receipt != scenario.want {
				t.Fatalf("observed receipt %#v, err %v, want success %v", receipt, err, scenario.success)
			}
			if runtime.readActive != scenario.active || runtime.checkedReady != scenario.ready {
				t.Fatalf("active=%v ready=%v, want active=%v ready=%v",
					runtime.readActive, runtime.checkedReady, scenario.active, scenario.ready)
			}
			after := observationFiles(t, path)
			if len(after) != len(before) {
				t.Fatal("observing changed the local journal files")
			}
			for name, info := range before {
				verified := after[name]
				if !os.SameFile(info, verified) || info.ModTime() != verified.ModTime() {
					t.Fatalf("observing rewrote %s", name)
				}
			}
		})
	}
}

func writeObservationFixture(
	t *testing.T, store *Store, current, next []byte, receipt, nextReceipt Receipt, unrecorded, stage bool,
) {
	t.Helper()
	dir, err := store.open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer dir.close()
	if err := dir.replace(StartupFilename, current); err != nil {
		t.Fatal(err)
	}
	if unrecorded {
		return
	}
	state := activation{Current: recordedConfig{Receipt: receipt, JSON: current}}
	if stage {
		state.Candidate = &recordedConfig{Receipt: nextReceipt, JSON: next}
	}
	if err := dir.save(state); err != nil {
		t.Fatal(err)
	}
}

func observationFiles(t *testing.T, path string) map[string]os.FileInfo {
	t.Helper()
	items, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]os.FileInfo, len(items))
	for _, item := range items {
		if result[item.Name()], err = item.Info(); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

type observationRuntime struct {
	t                        *testing.T
	expected                 Receipt
	loaded                   []byte
	err                      error
	readActive, checkedReady bool
}

func (runtime *observationRuntime) Active(_ context.Context, id string) ([]byte, error) {
	if id != runtime.expected.RuntimeID {
		runtime.t.Fatalf("answered for the wrong runtime %q", id)
	}
	runtime.readActive = true
	return runtime.loaded, nil
}

func (runtime *observationRuntime) Ready(_ context.Context, receipt Receipt) error {
	if !runtime.readActive || receipt != runtime.expected {
		runtime.t.Fatalf("checked readiness without the exact active receipt %#v", receipt)
	}
	runtime.checkedReady = true
	return runtime.err
}

func (runtime *observationRuntime) Validate(context.Context, string, []byte) error {
	runtime.t.Fatal("read-only observation tried to stage a configuration")
	return nil
}

func (runtime *observationRuntime) Reload(context.Context, string, []byte) error {
	runtime.t.Fatal("read-only observation tried to change the serving configuration")
	return nil
}

func (runtime *observationRuntime) Quiescent(context.Context, string) error {
	runtime.t.Fatal("read-only observation tried to resolve a reload")
	return nil
}
