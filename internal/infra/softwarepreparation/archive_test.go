package softwarepreparation

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// PKG-07: GitHub commit metadata must not reject a real source archive;
// accepting metadata must not permit a foreign commit or traversal member.
func TestSourceArchiveCommitMetadataAndTraversal(t *testing.T) {
	commit := strings.Repeat("a", 40)
	for _, scenario := range []struct {
		name, metadata, extra string
		wantError             bool
	}{
		{name: "GitHub metadata", metadata: commit},
		{name: "foreign commit", metadata: strings.Repeat("b", 40), wantError: true},
		{name: "traversal", metadata: commit, extra: "../escaped", wantError: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			root := t.TempDir()
			archivePath := filepath.Join(root, "source.tar.gz")
			file, err := os.Create(archivePath)
			if err != nil {
				t.Fatal(err)
			}
			compressed := gzip.NewWriter(file)
			writer := tar.NewWriter(compressed)
			if err := writer.WriteHeader(&tar.Header{Typeflag: tar.TypeXGlobalHeader,
				PAXRecords: map[string]string{"comment": scenario.metadata}}); err != nil {
				t.Fatal(err)
			}
			members := []string{"Dockerfile.build", "Dockerfile.agent", "VERSION", "Makefile"}
			if scenario.extra != "" {
				members = append(members, scenario.extra)
			}
			for _, member := range members {
				if err := writer.WriteHeader(&tar.Header{Name: "groundplane-" + commit + "/" + member,
					Typeflag: tar.TypeReg, Mode: 0o600, Size: 1}); err != nil {
					t.Fatal(err)
				}
				if _, err := writer.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
			}
			for _, close := range []func() error{writer.Close, compressed.Close, file.Close} {
				if err := close(); err != nil {
					t.Fatal(err)
				}
			}
			err = extractSourceArchive(archivePath, filepath.Join(root, "source"), commit)
			if (err != nil) != scenario.wantError {
				t.Fatalf("extract error = %v, want error %v", err, scenario.wantError)
			}
			if _, err := os.Stat(filepath.Join(root, "escaped")); !os.IsNotExist(err) {
				t.Fatal("archive member escaped the source directory")
			}
		})
	}
}
