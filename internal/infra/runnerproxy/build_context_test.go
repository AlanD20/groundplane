package runnerproxy

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"
)

type contextFixture struct {
	name string
	kind byte
	link string
	body string
}

func contextArchive(t *testing.T, entries ...contextFixture) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, entry := range entries {
		kind := entry.kind
		if kind == 0 {
			kind = tar.TypeReg
		}
		header := &tar.Header{
			Name: entry.name, Typeflag: kind, Mode: 0o644, Linkname: entry.link, Size: int64(len(entry.body)),
		}
		if err := writer.WriteHeader(header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(entry.body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func gzipContext(t *testing.T, input []byte) []byte {
	t.Helper()
	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(input); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func privateSpool(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

// RUN-04: the daemon receives validated bytes, not an unchecked copy of the
// input; compression, content identity and scratch cleanup must agree.
func TestBuildContextPreservesFilesAndSafeLinksWithoutLeavingScratch(t *testing.T) {
	archive := contextArchive(t,
		contextFixture{name: "./Dockerfile", body: "FROM scratch\nCOPY app /app\n"},
		contextFixture{name: "app", body: "application bytes"},
		contextFixture{name: "bin", kind: tar.TypeDir},
		contextFixture{name: "bin/app", kind: tar.TypeSymlink, link: "../app"},
		contextFixture{name: "app-copy", kind: tar.TypeLink, link: "app"},
	)
	for _, input := range [][]byte{archive, gzipContext(t, archive)} {
		root := privateSpool(t)
		prepared, err := PrepareBuildContext(context.Background(), root, bytes.NewReader(input))
		if err != nil {
			t.Fatal(err)
		}
		if !prepared.HasDockerfile("Dockerfile") || prepared.HasDockerfile("bin/app") {
			t.Fatal("Dockerfile selection did not require a validated regular file")
		}
		value, err := io.ReadAll(prepared)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(value)
		if prepared.Size() != int64(len(value)) || prepared.Digest() != "sha256:"+hex.EncodeToString(digest[:]) {
			t.Fatal("prepared content identity differs from forwarded bytes")
		}
		reader := tar.NewReader(bytes.NewReader(value))
		for _, expected := range []contextFixture{
			{name: "Dockerfile", body: "FROM scratch\nCOPY app /app\n"},
			{name: "app", body: "application bytes"}, {name: "bin", kind: tar.TypeDir},
			{name: "bin/app", kind: tar.TypeSymlink, link: "../app"},
			{name: "app-copy", kind: tar.TypeLink, link: "app"},
		} {
			header, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(reader)
			if err != nil || header.Name != expected.name || header.Linkname != expected.link ||
				string(body) != expected.body {
				t.Fatalf("forwarded entry differs from input: %s", expected.name)
			}
		}
		if err := prepared.Close(); err != nil {
			t.Fatal(err)
		}
		remaining, err := os.ReadDir(root)
		if err != nil || len(remaining) != 0 {
			t.Fatal("build left scratch files")
		}
	}
}

// RUN-04: neither entry order nor chained links may escape the build root;
// ambiguous targets and special files never reach the Docker extractor.
func TestBuildContextRejectsEscapesCyclesAndAmbiguousExtraction(t *testing.T) {
	cases := [][]contextFixture{
		{{name: "../outside", body: "escape"}},
		{{name: "/absolute", body: "escape"}},
		{{name: "file"}, {name: "./file"}},
		{{name: "pipe", kind: tar.TypeFifo}},
		{{name: "directory", kind: tar.TypeSymlink, link: ".."}},
		{{name: "a", kind: tar.TypeSymlink, link: "b"}, {name: "b", kind: tar.TypeSymlink, link: "a"}},
		{{name: "a", kind: tar.TypeSymlink, link: "."}, {name: "b", kind: tar.TypeSymlink, link: "a/../outside"}},
		{{name: "link", kind: tar.TypeSymlink, link: "target"}, {name: "link/file", body: "escape"}},
		{{name: "link/file", body: "escape"}, {name: "link", kind: tar.TypeSymlink, link: "target"}},
		{{name: "a", body: "not directory"}, {name: "a/b", body: "child"}},
		{{name: "hard", kind: tar.TypeLink, link: "missing"}},
		{{name: "hard", kind: tar.TypeLink, link: "link"}, {name: "link", kind: tar.TypeSymlink, link: "target"}},
	}
	for index, entries := range cases {
		root := privateSpool(t)
		prepared, err := PrepareBuildContext(context.Background(), root, bytes.NewReader(contextArchive(t, entries...)))
		if err == nil || prepared != nil {
			t.Fatalf("unsafe archive %d was accepted", index)
		}
		remaining, err := os.ReadDir(root)
		if err != nil || len(remaining) != 0 {
			t.Fatal("rejected context left scratch")
		}
	}
}

// RUN-04: a validator that stops at the first tar/gzip terminator must not
// forward a second hidden archive, a corrupt checksum or truncated content.
func TestBuildContextRejectsHiddenOrTruncatedStreams(t *testing.T) {
	archive := contextArchive(t, contextFixture{name: "Dockerfile", body: "FROM scratch"})
	compressed := gzipContext(t, archive)
	corrupt := bytes.Clone(compressed)
	corrupt[len(corrupt)-8] ^= 1
	inputs := [][]byte{
		{}, archive[:512+3], append(bytes.Clone(archive), archive...),
		append(bytes.Clone(compressed), compressed...), corrupt,
		append(bytes.Clone(compressed), 0), compressed[:len(compressed)-5],
	}
	for index, input := range inputs {
		prepared, err := PrepareBuildContext(context.Background(), privateSpool(t), bytes.NewReader(input))
		if err == nil || prepared != nil {
			t.Fatalf("invalid framing %d was accepted", index)
		}
	}
}

// RUN-04: bounded IO must reject overflow, including a reader that is called
// again after failure; cancellation must not return a usable build context.
func TestBuildContextBoundsAndCancellation(t *testing.T) {
	reader := &contextReader{ctx: context.Background(), reader: bytes.NewBufferString("12345"), remaining: 4}
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("encoded limit was ignored")
	}
	if _, err := reader.Read(make([]byte, 3)); err == nil {
		t.Fatal("exhausted reader resumed")
	}
	var buffer bytes.Buffer
	writer := &contextWriter{ctx: context.Background(), writer: &buffer, remaining: 4}
	if _, err := writer.Write([]byte("12345")); err == nil || buffer.Len() != 0 {
		t.Fatal("oversized output was partially accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	prepared, err := PrepareBuildContext(ctx, privateSpool(t), bytes.NewReader([]byte("archive")))
	if prepared != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled context result: %v", err)
	}
}
