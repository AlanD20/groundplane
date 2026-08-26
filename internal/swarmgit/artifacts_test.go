package swarmgit

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/AlanD20/groundplane/internal/swarmcheck"
	"golang.org/x/sys/unix"
)

// Rationale: artifact writes must never escape the wave root through a symlinked parent.
func TestArtifactWriteRejectsSymlinkParent(t *testing.T) {
	root := t.TempDir()
	attacker := t.TempDir()
	waveRoot := filepath.Join(root, ".tmp", "swarm", "wave-1")
	if err := os.MkdirAll(waveRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(attacker, filepath.Join(waveRoot, "artifacts")); err != nil {
		t.Fatal(err)
	}
	inspector := NewInspector(root, nil)
	path := swarmcheck.ArtifactRoot("wave-1") + "/report.json"
	if err := inspector.writeArtifactAtomically(
		context.Background(),
		"wave-1",
		path,
		[]byte("safe"),
	); err == nil {
		t.Fatal("artifact write followed a symlinked parent")
	}
	if _, err := os.Stat(filepath.Join(attacker, "report.json")); !os.IsNotExist(err) {
		t.Fatalf("attacker path was modified: %v", err)
	}
}

// Rationale: atomic writes must remain anchored to the pinned directory across path swaps.
func TestPinnedArtifactDirectorySurvivesParentSymlinkSwap(t *testing.T) {
	root := t.TempDir()
	attacker := t.TempDir()
	artifacts := filepath.Join(root, ".tmp", "swarm", "wave-1", "artifacts")
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	directoryFD, err := openDirectoryBeneath(context.Background(), root, ".tmp/swarm/wave-1/artifacts", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(directoryFD) })
	pinned := artifacts + "-pinned"
	if err := os.Rename(artifacts, pinned); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(attacker, artifacts); err != nil {
		t.Fatal(err)
	}
	if err := writeArtifactAtDirectory(
		context.Background(),
		directoryFD,
		"report.json",
		[]byte("safe"),
	); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(pinned, "report.json"))
	if err != nil || string(data) != "safe" {
		t.Fatalf("pinned artifact = %q, %v", data, err)
	}
	if _, err := os.Stat(filepath.Join(attacker, "report.json")); !os.IsNotExist(err) {
		t.Fatalf("symlink target was modified: %v", err)
	}
}

// Rationale: artifact reads must remain descriptor-relative during concurrent symlink swaps.
func TestArtifactReadNeverFollowsSwappedSymlinkParent(t *testing.T) {
	root := t.TempDir()
	attacker := t.TempDir()
	artifacts := filepath.Join(root, ".tmp", "swarm", "wave-1", "artifacts")
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(artifacts, "report.json"),
		[]byte("safe"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(attacker, "report.json"),
		[]byte("attacker"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		pinned := artifacts + "-pinned"
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := os.Rename(artifacts, pinned); err != nil {
				continue
			}
			if err := os.Symlink(attacker, artifacts); err == nil {
				_ = os.Remove(artifacts)
			}
			_ = os.Rename(pinned, artifacts)
		}
	}()
	inspector := NewInspector(root, nil)
	path := swarmcheck.ArtifactRoot("wave-1") + "/report.json"
	for range 500 {
		data, _, err := inspector.readArtifact(context.Background(), "wave-1", path)
		if err == nil && string(data) != "safe" {
			close(stop)
			<-done
			t.Fatalf("read bytes through swapped parent: %q", data)
		}
	}
	close(stop)
	<-done
}

// Rationale: cancellation before rename must preserve the prior immutable artifact bytes.
func TestAtomicArtifactWriteFailurePreservesExistingFile(t *testing.T) {
	root := t.TempDir()
	artifacts := filepath.Join(root, ".tmp", "swarm", "wave-1", "artifacts")
	if err := os.MkdirAll(artifacts, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(artifacts, "report.json")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	directoryFD, err := openDirectoryBeneath(context.Background(), root, ".tmp/swarm/wave-1/artifacts", false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(directoryFD) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writeArtifactAtDirectory(
		ctx,
		directoryFD,
		"report.json",
		[]byte("replacement"),
	); err == nil {
		t.Fatal("canceled atomic write succeeded")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "original" {
		t.Fatalf("existing artifact after failed write = %q, %v", data, err)
	}
}
