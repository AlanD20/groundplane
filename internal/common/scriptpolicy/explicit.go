// Package scriptpolicy owns the closed explicit-runner bounds shared by desired
// validation and the authenticated Controller/Agent execution-plan boundary.
package scriptpolicy

import (
	"path"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumVolumes = 32
	MaximumEntries = 64
	BodyTarget     = "/groundplane-script-body"
)

// NumericUser accepts canonical decimal uint32 uid:gid, with no ambient default.
func NumericUser(value string) (uint32, uint32, error) {
	user, group, found := strings.Cut(value, ":")
	uid, uidErr := strconv.ParseUint(user, 10, 32)
	gid, gidErr := strconv.ParseUint(group, 10, 32)
	if !found || uidErr != nil || gidErr != nil ||
		strconv.FormatUint(uid, 10) != user || strconv.FormatUint(gid, 10) != group {
		return 0, 0, errs.New(errs.KindValidationFailed, "script execution user must be canonical numeric uid:gid")
	}
	return uint32(uid), uint32(gid), nil
}

// PathsOverlap compares whole path components, not character-prefix siblings.
// Callers validate canonical absolute paths before comparing them.
func PathsOverlap(left, right string) bool {
	return left == right || strings.HasPrefix(left, right+"/") || strings.HasPrefix(right, left+"/") ||
		left == "/" || right == "/"
}

// ValidateMountTarget protects the fixed body, interpreter, kernel, runtime and
// Docker-owned files while allowing application paths such as /etc/tls.
func ValidateMountTarget(target string) error {
	if !utf8.ValidString(target) || strings.IndexFunc(target, unicode.IsControl) >= 0 ||
		!path.IsAbs(target) || target == "/" || path.Clean(target) != target {
		return errs.New(errs.KindValidationFailed, "script execution mount target must be a canonical absolute path")
	}
	for _, reserved := range []string{
		BodyTarget, "/proc", "/sys", "/dev", "/run", "/var/run",
		"/bin", "/sbin", "/usr", "/lib", "/lib64",
		"/etc/hosts", "/etc/hostname", "/etc/resolv.conf",
	} {
		if PathsOverlap(target, reserved) {
			return errs.New(errs.KindValidationFailed, "script execution mount overlaps a reserved target")
		}
	}
	return nil
}
