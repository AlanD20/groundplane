package swarmcheck

import (
	"fmt"
	"path"
	"strings"
)

// PrimaryOnlyPaths returns the immutable exact and prefix deny set. The
// returned slice is a copy so callers cannot mutate the compiled policy.
func PrimaryOnlyPaths() []RepoPath {
	return append([]RepoPath(nil), primaryOnlyPaths...)
}

var primaryOnlyPaths = []RepoPath{
	".agents",
	".codex",
	".git",
	".github",
	".gitignore",
	".node-version",
	".tmp",
	"AGENTS.md",
	".dockerignore",
	"Dockerfile",
	"Dockerfile.agent",
	"Makefile",
	"architecture-baseline.json",
	"cmd/architecture-check",
	"cmd/swarm-check",
	"docs",
	"go.mod",
	"go.sum",
	"internal/architecturecheck",
	"internal/swarmcheck",
	"internal/swarmgit",
	"openapi.json",
	"proto/agent.proto",
	"proto/agentpb",
	"staticcheck.conf",
	"console/src/lib/api.generated.ts",
	"console/dist",
	"internal/cli/apiclient/generated",
}

var compiledSharedAreas = map[SharedAreaName][]RepoPath{
	"console-store":      {"console/src/lib/store.tsx"},
	"console-types":      {"console/src/lib/types.ts"},
	"controller-server":  {"internal/controller/server.go"},
	"controller-console": {"internal/controller/console.go"},
	"cli-router":         {"internal/cli/router.go"},
}

// SharedAreaDefinitions returns the closed exact path sets that a manifest
// may assign to one lane. Paths are copies to keep the compiled policy fixed.
func SharedAreaDefinitions() map[SharedAreaName][]RepoPath {
	result := make(map[SharedAreaName][]RepoPath, len(compiledSharedAreas))
	for name, paths := range compiledSharedAreas {
		result[name] = append([]RepoPath(nil), paths...)
	}
	return result
}

// IsPrimaryOnlyPath reports whether a canonical changed path is protected by
// an unconditional primary-only exact path or directory prefix.
func IsPrimaryOnlyPath(value RepoPath) bool {
	if err := validateLeasePath(value); err != nil {
		return false
	}
	for _, protected := range primaryOnlyPaths {
		if value == protected || strings.HasPrefix(string(value), string(protected)+"/") {
			return true
		}
	}
	return false
}

func overlapsPrimaryOnlyPath(value RepoPath) bool {
	if err := validateLeasePath(value); err != nil {
		return false
	}
	for _, protected := range primaryOnlyPaths {
		if pathsOverlap(value, protected) {
			return true
		}
	}
	return false
}

// ArtifactRoot is the only location where reports for a wave may be stored.
func ArtifactRoot(wave WaveID) RepoPath {
	return RepoPath(".tmp/swarm/" + string(wave) + "/artifacts")
}

// WorktreeRoot is the only location where writer worktrees may be derived.
func WorktreeRoot(wave WaveID) RepoPath {
	return RepoPath(".tmp/swarm/" + string(wave) + "/worktrees")
}

// WorktreePath derives a writer's required detached worktree path.
func WorktreePath(wave WaveID, lane Lane) RepoPath {
	return RepoPath(string(WorktreeRoot(wave)) + "/" + string(lane))
}

// ReserveWorktreePath derives one remediation reserve's temporary worktree.
func ReserveWorktreePath(wave WaveID, name ReserveName) RepoPath {
	return RepoPath(string(WorktreeRoot(wave)) + "/reserve-" + string(name))
}

// ManifestPath derives the one primary-owned manifest path.
func ManifestPath(wave WaveID) RepoPath {
	return RepoPath(".tmp/swarm/" + string(wave) + "/manifest.json")
}

// SnapshotNamespace is the complete temporary ref namespace for one wave.
func SnapshotNamespace(wave WaveID) Ref {
	return Ref("refs/heads/groundplane/swarm/" + string(wave) + "/")
}

// SnapshotRef derives the primary-owned immutable ref for one lane attempt.
func SnapshotRef(wave WaveID, lane Lane, attempt AttemptID) Ref {
	return Ref(string(SnapshotNamespace(wave)) + string(lane) + "/" + string(attempt))
}

// GateArtifactPath derives the sole typed evidence path for one closed gate.
func GateArtifactPath(wave WaveID, gate GateID) RepoPath {
	return RepoPath(string(ArtifactRoot(wave)) + "/gate-" + string(gate) + ".json")
}

// ValidateArtifactPath validates a report path against the derived artifact
// root. It rejects symlink-like traversal and accepts only regular relative
// paths beneath the root.
func ValidateArtifactPath(wave WaveID, value RepoPath) error {
	if err := validateLeasePath(value); err != nil {
		return err
	}
	root := ArtifactRoot(wave)
	if value == root || !strings.HasPrefix(string(value), string(root)+"/") {
		return invalid(fmt.Sprintf("artifact path must be beneath %s", root))
	}
	return nil
}

// ValidatePath validates one canonical repository-relative path.
func ValidatePath(value RepoPath) error { return validateLeasePath(value) }

// ParseRepoPath validates a repository path observed at a process boundary.
func ParseRepoPath(value string) (RepoPath, error) {
	path := RepoPath(value)
	if err := ValidatePath(path); err != nil {
		return "", err
	}
	return path, nil
}

// ChangeAllowed applies the compiled primary-only policy, closed shared-area
// ownership, and the selected writer's exclusive leases to one path.
func ChangeAllowed(manifest Manifest, lane Lane, value RepoPath) bool {
	if validateLeasePath(value) != nil || IsPrimaryOnlyPath(value) {
		return false
	}
	for areaName, sharedPaths := range compiledSharedAreas {
		for _, shared := range sharedPaths {
			if value != shared {
				continue
			}
			declared := false
			for _, area := range manifest.SharedAreas {
				if area.Name == areaName {
					declared = area.OwnerLane == lane
				}
			}
			return declared
		}
	}
	for _, area := range manifest.SharedAreas {
		for _, shared := range area.Paths {
			if value == shared {
				return area.OwnerLane == lane
			}
		}
	}
	for _, writer := range manifest.Writers {
		if writer.Lane != lane {
			continue
		}
		for _, lease := range writer.Leases {
			if pathWithin(value, lease) {
				return true
			}
		}
	}
	return false
}

// ChangeAllowedForSnapshot applies an attempt's narrow remediation lease when
// present and otherwise applies the lane's original lease policy.
func ChangeAllowedForSnapshot(manifest Manifest, snapshot Snapshot, value RepoPath) bool {
	if snapshot.RemediationLease != "" {
		return validateLeasePath(value) == nil && pathWithin(value, snapshot.RemediationLease)
	}
	return ChangeAllowed(manifest, snapshot.Lane, value)
}

// ResolveWritingAssignment identifies the stopped worktree and expected HEAD
// for the lane's selected attempt. A lane without an attempt uses its writer.
func ResolveWritingAssignment(manifest Manifest, lane Lane) (WritingAssignment, bool) {
	var writer Writer
	foundWriter := false
	for _, candidate := range manifest.Writers {
		if candidate.Lane == lane {
			writer = candidate
			foundWriter = true
			break
		}
	}
	if !foundWriter {
		return WritingAssignment{}, false
	}
	assignment := WritingAssignment{Worktree: writer.Worktree, Head: writer.Base}
	var selected CommitID
	for _, active := range manifest.Active {
		if active.Lane == lane {
			selected = active.Snapshot
			break
		}
	}
	if selected == "" {
		return assignment, true
	}
	for _, snapshot := range manifest.Snapshots {
		if snapshot.Commit != selected {
			continue
		}
		assignment.Head = snapshot.Parent
		assignment.Attempt = snapshot
		if snapshot.Author == writer.Identity {
			return assignment, true
		}
		for _, reserve := range manifest.Reserves {
			if reserve.Identity == snapshot.Author {
				assignment.Worktree = reserve.Worktree
				return assignment, true
			}
		}
		return WritingAssignment{}, false
	}
	return WritingAssignment{}, false
}

func validateLeasePath(value RepoPath) error {
	raw := string(value)
	if value == "" || len(value) > maxPathBytes || !isASCII(raw) ||
		strings.ContainsRune(raw, '\x00') ||
		strings.Contains(raw, "\\") ||
		strings.HasPrefix(raw, "/") ||
		path.Clean(raw) != raw {
		return invalid("path must be canonical ASCII slash-relative")
	}
	for _, segment := range strings.Split(raw, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return invalid("path must be canonical ASCII slash-relative")
		}
	}
	return nil
}

func isASCII(value string) bool {
	for index := 0; index < len(value); index++ {
		if value[index] > 0x7f {
			return false
		}
	}
	return true
}

func pathsOverlap(left, right RepoPath) bool {
	return left == right || strings.HasPrefix(string(left), string(right)+"/") ||
		strings.HasPrefix(string(right), string(left)+"/")
}

func pathWithin(value, lease RepoPath) bool {
	return value == lease || strings.HasPrefix(string(value), string(lease)+"/")
}

func validIdentifier[T ~string](value T) bool {
	raw := string(value)
	if raw == "" || len(raw) > maxIdentifierBytes || !isASCII(raw) {
		return false
	}
	for index, character := range []byte(raw) {
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			character >= '0' && character <= '9' ||
			index > 0 && (character == '-' || character == '_' || character == '.') {
			continue
		}
		return false
	}
	return true
}

func validRefSegment[T ~string](value T) bool {
	raw := string(value)
	if raw == "" || len(raw) > maxIdentifierBytes {
		return false
	}
	for index, character := range []byte(raw) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			continue
		}
		if index > 0 && (character == '-' || character == '_') {
			continue
		}
		return false
	}
	return true
}
