//go:build linux

// Command postgres16releasemeta writes the native image's measured release
// descriptor. It is build tooling, not a mode exposed by the runtime helper.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/postgres16helper"
)

func main() {
	root := flag.String("root", "", "native image filesystem root")
	output := flag.String("output", "", "new manifest file")
	flag.Parse()
	if err := writeManifest(*root, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writeManifest(root, output string) error {
	if flag.NArg() != 0 || !filepath.IsAbs(root) || filepath.Clean(root) != root ||
		!filepath.IsAbs(output) || (runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64") {
		return fmt.Errorf("postgres release: absolute root/output and native amd64 or arm64 are required")
	}
	manifest := postgres16protocol.ManagedReleaseManifest{
		Schema: 1, OS: "linux", Architecture: runtime.GOARCH, PostgreSQLMajor: postgres16protocol.PostgreSQLMajor,
	}
	files := []struct {
		path string
		into *string
	}{
		{postgres16protocol.HelperPath, &manifest.HelperSHA256},
		{postgres16protocol.ClientGatePath, &manifest.GateSHA256},
		{postgres16protocol.PGDumpPath, &manifest.PGDumpSHA256},
		{postgres16protocol.PGRestorePath, &manifest.PGRestoreSHA256},
		{postgres16protocol.PSQLPath, &manifest.PSQLSHA256},
	}
	for _, item := range files {
		digest, err := digestFile(filepath.Join(root, item.path))
		if err != nil {
			return err
		}
		*item.into = digest
	}
	profile := postgres16protocol.ManagedLaunchProfileSHA256()
	manifest.LaunchProfileSHA256 = hex.EncodeToString(profile[:])
	seccomp, err := postgres16helper.GateSeccompSHA256()
	if err != nil {
		return err
	}
	manifest.GateSeccompSHA256 = hex.EncodeToString(seccomp[:])
	if _, err := manifest.Authority(); err != nil {
		return err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o444)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func digestFile(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return "", fmt.Errorf("postgres release: expected a regular executable: %s", path)
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	n, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", closeErr
	}
	if n != info.Size() {
		return "", fmt.Errorf("postgres release: executable changed while measuring")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
