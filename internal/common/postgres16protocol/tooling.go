package postgres16protocol

import (
	"path/filepath"
	"regexp"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imageref"
)

const HostToolsRoot = "/root/.groundplane/postgres16-tools"

var databaseImagePattern = regexp.MustCompile(
	`^(?:(?:docker\.io|registry-1\.docker\.io)/)?(?:library/)?postgres:16(?:\.[0-9]+)?-alpine(?:[0-9]+\.[0-9]+)?(?:@sha256:[0-9a-f]{64})?$`,
)

// ValidDatabaseImage limits managed patches to the official PostgreSQL 16
// Alpine family, retaining its data location and database uid/gid contract.
func ValidDatabaseImage(reference string) bool {
	return databaseImagePattern.MatchString(reference)
}

// ToolsDirectory is installation-owned runtime state, never Blueprint input.
// Each authenticated toolbox has a separate immutable host directory.
func ToolsDirectory(reference string) (string, error) {
	if !imageref.IsDigestPinned(reference) {
		return "", invalidConfinement("PostgreSQL backup tools must be digest-pinned")
	}
	_, digest, _ := strings.Cut(reference, "@sha256:")
	if len(digest) != 64 {
		return "", invalidConfinement("PostgreSQL backup tools digest is invalid")
	}
	return filepath.Join(HostToolsRoot, digest), nil
}
