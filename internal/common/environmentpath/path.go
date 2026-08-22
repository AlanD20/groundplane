// Package environmentpath owns the one pure stable-ID derivation for an
// Environment's host volume directory. Filesystem authorization and mutation
// are separate side-effecting layers; a stored absolute path alone is never
// authority.
package environmentpath

import (
	"path"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const DefaultVolumeRoot = "/var/lib/groundplane/vol"

type ProjectKind string

const (
	ProjectKindTenant  ProjectKind = "tenant"
	ProjectKindBacking ProjectKind = "backing"
)

type Scope struct {
	ProjectKind   ProjectKind
	TenantID      string
	ProjectID     string
	EnvironmentID string
}

// ValidateRoot applies the side-effect-free portion of ADR 0025's configured
// root contract. Descriptor-relative existence, metadata, and symlink checks
// belong to Controller startup and the task-scoped helper.
func ValidateRoot(root string) error {
	if root == "" || !utf8.ValidString(root) || strings.IndexByte(root, 0) >= 0 ||
		!strings.HasPrefix(root, "/") || path.Clean(root) != root || root == "/" {
		return errs.New(
			errs.KindValidationFailed,
			"environment volume root must be a clean, non-root absolute UTF-8 path",
		)
	}
	return nil
}

// Derive returns the exact redundant volume_dir invariant for one durable
// Environment scope. Mutable names and slugs are intentionally absent.
func Derive(root string, scope Scope) (string, error) {
	if err := ValidateRoot(root); err != nil {
		return "", err
	}
	if err := ids.Validate(ids.KindProject, scope.ProjectID); err != nil {
		return "", errs.New(errs.KindValidationFailed, "environment volume scope has an invalid Project id")
	}
	if err := ids.Validate(ids.KindEnvironment, scope.EnvironmentID); err != nil {
		return "", errs.New(errs.KindValidationFailed, "environment volume scope has an invalid Environment id")
	}
	switch scope.ProjectKind {
	case ProjectKindTenant:
		if err := ids.Validate(ids.KindTenant, scope.TenantID); err != nil {
			return "", errs.New(errs.KindValidationFailed, "tenant Environment volume scope requires a valid Tenant id")
		}
		return path.Join(root, scope.TenantID, scope.ProjectID, scope.EnvironmentID), nil
	case ProjectKindBacking:
		if scope.TenantID != "" {
			return "", errs.New(errs.KindValidationFailed, "backing Environment volume scope must not have a Tenant id")
		}
		return path.Join(root, "platform", scope.ProjectID, scope.EnvironmentID), nil
	default:
		return "", errs.New(errs.KindValidationFailed, "environment volume scope has an invalid Project kind")
	}
}

// Parse verifies that volumeDir is exactly one derived tenant or backing path
// below root and returns its typed stable-ID ownership. The root is trusted
// daemon policy; volumeDir is untrusted Task or durable-record data.
func Parse(root string, volumeDir string) (Scope, error) {
	if err := ValidateRoot(root); err != nil {
		return Scope{}, err
	}
	if volumeDir == "" || !utf8.ValidString(volumeDir) || strings.IndexByte(volumeDir, 0) >= 0 ||
		path.Clean(volumeDir) != volumeDir {
		return Scope{}, errs.New(errs.KindValidationFailed, "environment volume directory is invalid")
	}
	relative, ok := strings.CutPrefix(volumeDir, root+"/")
	if !ok {
		return Scope{}, errs.New(errs.KindValidationFailed, "environment volume directory is outside the configured root")
	}
	components := strings.Split(relative, "/")
	if len(components) != 3 {
		return Scope{}, errs.New(errs.KindValidationFailed, "environment volume directory has an invalid ownership shape")
	}
	scope := Scope{ProjectID: components[1], EnvironmentID: components[2]}
	if components[0] == "platform" {
		scope.ProjectKind = ProjectKindBacking
	} else {
		scope.ProjectKind = ProjectKindTenant
		scope.TenantID = components[0]
	}
	derived, err := Derive(root, scope)
	if err != nil {
		return Scope{}, err
	}
	if derived != volumeDir {
		return Scope{}, errs.New(errs.KindValidationFailed, "environment volume directory is not canonical")
	}
	return scope, nil
}
