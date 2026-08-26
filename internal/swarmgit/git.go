package swarmgit

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/swarmcheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	gitExecutable     = "/usr/bin/git"
	maxGitOutputBytes = 2 << 20
	gitCommandTimeout = 2 * time.Minute
)

type worktreeEvidence struct {
	Head     swarmcheck.CommitID
	Detached bool
}

type statusEvidence struct {
	Paths           []statusPath
	Staged          bool
	Unstaged        bool
	Unmerged        bool
	Sparse          bool
	AssumeUnchanged bool
}

type statusKind string

const (
	statusIgnored   statusKind = "ignored"
	statusUntracked statusKind = "untracked"
	statusTracked   statusKind = "tracked"
)

type statusPath struct {
	Status statusKind
	Path   swarmcheck.RepoPath
}

func (inspector *Inspector) requireClean(ctx context.Context, includeIgnored bool) error {
	clean, err := inspector.clean(ctx, includeIgnored)
	if err != nil {
		return err
	}
	if !clean {
		return invalid("repository worktree or index is not clean")
	}
	return nil
}

func (inspector *Inspector) clean(ctx context.Context, includeIgnored bool) (bool, error) {
	status, err := inspector.status(ctx, includeIgnored)
	if err != nil {
		return false, err
	}
	if status.Staged || status.Unmerged || status.Sparse || status.AssumeUnchanged {
		return false, nil
	}
	for _, changed := range status.Paths {
		if includeIgnored || changed.Status != statusIgnored {
			return false, nil
		}
	}
	return true, nil
}

func (inspector *Inspector) requireTargetRef(ctx context.Context, target swarmcheck.Ref) error {
	result, err := inspector.git(ctx, "symbolic-ref", "--quiet", "HEAD")
	if err != nil {
		return err
	}
	actual, err := swarmcheck.ParseRef(strings.TrimSpace(string(result.Stdout)))
	if err != nil || actual != target {
		return invalid("primary HEAD is not attached to target_ref")
	}
	return nil
}

func (inspector *Inspector) resolve(ctx context.Context, ref string) (swarmcheck.CommitID, error) {
	result, err := inspector.git(ctx, "rev-parse", "--verify", "--quiet", ref+"^{commit}")
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(result.Stdout))
	return swarmcheck.ParseCommitID(value)
}

type headAttachment struct {
	Ref      swarmcheck.Ref
	Detached bool
}

func (inspector *Inspector) headAttachment(ctx context.Context) (headAttachment, error) {
	result, err := inspector.git(ctx, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return headAttachment{}, err
	}
	value := strings.TrimSpace(string(result.Stdout))
	if value == "HEAD" {
		return headAttachment{Detached: true}, nil
	}
	ref, err := swarmcheck.ParseRef(value)
	if err != nil {
		return headAttachment{}, err
	}
	return headAttachment{Ref: ref}, nil
}

func (inspector *Inspector) writeTree(ctx context.Context) (swarmcheck.TreeID, error) {
	result, err := inspector.git(ctx, "write-tree")
	if err != nil {
		return "", err
	}
	value := strings.TrimSpace(string(result.Stdout))
	return swarmcheck.ParseTreeID(value)
}

func (inspector *Inspector) commit(ctx context.Context, ref string) (CommitEvidence, error) {
	result, err := inspector.git(
		ctx,
		"show",
		"--no-patch",
		"--format=%H%x00%P%x00%T%x00%G?%x00%GF",
		ref,
	)
	if err != nil {
		return CommitEvidence{}, err
	}
	fields := strings.Split(strings.TrimSuffix(string(result.Stdout), "\n"), "\x00")
	if len(fields) != 5 {
		return CommitEvidence{}, invalid("git commit metadata is malformed")
	}
	commit, err := swarmcheck.ParseCommitID(fields[0])
	if err != nil {
		return CommitEvidence{}, err
	}
	parents := make([]swarmcheck.CommitID, 0)
	for _, value := range strings.Fields(fields[1]) {
		parent, err := swarmcheck.ParseCommitID(value)
		if err != nil {
			return CommitEvidence{}, err
		}
		parents = append(parents, parent)
	}
	tree, err := swarmcheck.ParseTreeID(fields[2])
	if err != nil {
		return CommitEvidence{}, err
	}
	fingerprint, err := swarmcheck.ParseFingerprint(strings.ToLower(fields[4]))
	if err != nil {
		return CommitEvidence{}, err
	}
	evidence := CommitEvidence{
		Commit:            commit,
		Parents:           parents,
		Tree:              tree,
		Signature:         fields[3],
		SignerFingerprint: fingerprint,
	}
	if !swarmcheckSignatureOK(evidence.Signature) {
		return CommitEvidence{}, invalid("git commit metadata is not a verified signed object")
	}
	return evidence, nil
}

func (inspector *Inspector) diff(
	ctx context.Context,
	base, snapshot swarmcheck.CommitID,
) ([]swarmcheck.ChangedPath, error) {
	result, err := inspector.git(
		ctx,
		"diff",
		"--name-status",
		"-z",
		"--no-renames",
		"--no-ext-diff",
		"--no-textconv",
		"--no-color",
		string(base),
		string(snapshot),
	)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(string(result.Stdout), "\x00")
	changed := make([]swarmcheck.ChangedPath, 0)
	for index := 0; index < len(parts); {
		if parts[index] == "" {
			break
		}
		if index+1 >= len(parts) {
			return nil, invalid("git diff metadata is truncated")
		}
		status, value := parts[index], parts[index+1]
		if !validDiffStatus(status) {
			return nil, invalid("git diff status is invalid")
		}
		path, err := swarmcheck.ParseRepoPath(value)
		if err != nil {
			return nil, invalid(fmt.Sprintf("git diff path is invalid: %s", value))
		}
		changed = append(changed, swarmcheck.ChangedPath{Path: path})
		index += 2
	}
	return changed, nil
}

type treeEntry struct {
	Mode string
	Type string
}

func (inspector *Inspector) validateChangedModes(
	ctx context.Context,
	base swarmcheck.CommitID,
	snapshot swarmcheck.CommitID,
	changed []swarmcheck.ChangedPath,
) error {
	for _, path := range changed {
		baseEntries, err := inspector.treeEntries(ctx, base, path.Path)
		if err != nil {
			return err
		}
		snapshotEntries, err := inspector.treeEntries(ctx, snapshot, path.Path)
		if err != nil {
			return err
		}
		for _, entry := range append(baseEntries, snapshotEntries...) {
			if entry.Mode != "100644" && entry.Mode != "100755" || entry.Type != "blob" {
				return invalid(fmt.Sprintf(
					"changed path %s has a symlink, submodule, or special Git mode",
					path.Path,
				))
			}
		}
	}
	return nil
}

func (inspector *Inspector) treeEntries(
	ctx context.Context,
	treeish swarmcheck.CommitID,
	value swarmcheck.RepoPath,
) ([]treeEntry, error) {
	result, err := inspector.git(
		ctx, "ls-tree", "-rz", "--full-tree", string(treeish), "--", string(value),
	)
	if err != nil {
		return nil, err
	}
	entries := make([]treeEntry, 0, 1)
	for _, record := range strings.Split(string(result.Stdout), "\x00") {
		if record == "" {
			continue
		}
		metadata := strings.SplitN(record, "\t", 2)
		if len(metadata) != 2 || metadata[1] != string(value) {
			return nil, invalid(fmt.Sprintf("git tree metadata is malformed for %s", value))
		}
		fields := strings.Fields(metadata[0])
		if len(fields) != 3 {
			return nil, invalid(fmt.Sprintf("git tree metadata is malformed for %s", value))
		}
		entries = append(entries, treeEntry{Mode: fields[0], Type: fields[1]})
	}
	return entries, nil
}

func (inspector *Inspector) status(
	ctx context.Context,
	includeIgnored bool,
) (statusEvidence, error) {
	args := []string{"status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all"}
	if includeIgnored {
		args = append(args, "--ignored=matching")
	}
	result, err := inspector.git(ctx, args...)
	if err != nil {
		return statusEvidence{}, err
	}
	evidence := statusEvidence{}
	parts := strings.Split(string(result.Stdout), "\x00")
	for index := 0; index < len(parts); index++ {
		record := parts[index]
		if record == "" {
			continue
		}
		if strings.HasPrefix(record, "?? ") || strings.HasPrefix(record, "!! ") {
			ignored := strings.HasPrefix(record, "!! ")
			path := strings.TrimPrefix(strings.TrimPrefix(record, "?? "), "!! ")
			if ignored {
				path = strings.TrimSuffix(path, "/")
			}
			canonical, err := swarmcheck.ParseRepoPath(path)
			if err != nil {
				return statusEvidence{}, invalid(
					fmt.Sprintf("git status path is invalid: %s", path),
				)
			}
			if ignored {
				continue
			}
			evidence.Paths = append(
				evidence.Paths,
				statusPath{Status: statusUntracked, Path: canonical},
			)
			continue
		}
		if len(record) < 4 {
			return statusEvidence{}, invalid("git status record is malformed")
		}
		x, y := record[0], record[1]
		rawPath := record[3:]
		path, err := swarmcheck.ParseRepoPath(rawPath)
		if err != nil {
			return statusEvidence{}, invalid(fmt.Sprintf("git status path is invalid: %s", rawPath))
		}
		if x != ' ' {
			evidence.Staged = true
		}
		if y != ' ' {
			evidence.Unstaged = true
		}
		if x == 'U' || y == 'U' || x == 'A' && y == 'A' {
			evidence.Unmerged = true
		}
		evidence.Paths = append(evidence.Paths, statusPath{Status: statusTracked, Path: path})
		if x == 'R' || x == 'C' {
			if index+1 >= len(parts) {
				return statusEvidence{}, invalid("git rename status is truncated")
			}
			index++
		}
	}
	flags, err := inspector.indexFlags(ctx)
	if err != nil {
		return statusEvidence{}, err
	}
	evidence.Sparse = flags.Sparse
	evidence.AssumeUnchanged = flags.AssumeUnchanged
	if includeIgnored {
		ignored, err := inspector.ignoredPaths(ctx)
		if err != nil {
			return statusEvidence{}, err
		}
		evidence.Paths = append(evidence.Paths, ignored...)
	}
	return evidence, nil
}

func (inspector *Inspector) ignoredPaths(ctx context.Context) ([]statusPath, error) {
	result, err := inspector.git(
		ctx,
		"ls-files",
		"--others",
		"--ignored",
		"--exclude-standard",
		"-z",
	)
	if err != nil {
		return nil, err
	}
	if !utf8.Valid(result.Stdout) {
		return nil, invalid("git ignored-path metadata is not valid UTF-8")
	}
	paths := make([]statusPath, 0)
	for _, value := range strings.Split(string(result.Stdout), "\x00") {
		if value == "" {
			continue
		}
		path, err := swarmcheck.ParseRepoPath(value)
		if err != nil {
			return nil, invalid(fmt.Sprintf("git ignored path is invalid: %s", value))
		}
		paths = append(paths, statusPath{Status: statusIgnored, Path: path})
	}
	return paths, nil
}

type indexFlags struct {
	Sparse          bool
	AssumeUnchanged bool
}

func (inspector *Inspector) indexFlags(ctx context.Context) (indexFlags, error) {
	result, err := inspector.git(ctx, "ls-files", "-v", "-z")
	if err != nil {
		return indexFlags{}, err
	}
	if !utf8.Valid(result.Stdout) {
		return indexFlags{}, invalid("git index metadata is not valid UTF-8")
	}
	flags := indexFlags{}
	for _, record := range strings.Split(string(result.Stdout), "\x00") {
		if len(record) == 0 {
			continue
		}
		switch record[0] {
		case 'S':
			flags.Sparse = true
		case 'h':
			flags.AssumeUnchanged = true
		}
	}
	return flags, nil
}

func (inspector *Inspector) worktrees(ctx context.Context) (map[swarmcheck.RepoPath]worktreeEvidence, error) {
	result, err := inspector.git(ctx, "worktree", "list", "--porcelain")
	if err != nil {
		return nil, err
	}
	if len(result.Stdout) > maxGitOutputBytes {
		return nil, invalid("git worktree metadata exceeds bound")
	}
	resultMap := make(map[swarmcheck.RepoPath]worktreeEvidence)
	var current *worktreeEvidence
	var currentPath swarmcheck.RepoPath
	appendCurrent := func() {
		if current != nil && currentPath != "" {
			resultMap[currentPath] = *current
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(string(result.Stdout)), "\n") {
		switch {
		case strings.HasPrefix(line, "worktree "):
			appendCurrent()
			rawPath := filepath.Clean(strings.TrimPrefix(line, "worktree "))
			relative, relativeErr := filepath.Rel(inspector.Root, rawPath)
			if relativeErr == nil && relative != ".." &&
				!strings.HasPrefix(relative, ".."+string(filepath.Separator)) && relative != "." {
				parsed, parseErr := swarmcheck.ParseRepoPath(filepath.ToSlash(relative))
				if parseErr != nil {
					return nil, parseErr
				}
				currentPath = parsed
			} else {
				currentPath = ""
			}
			current = &worktreeEvidence{}
		case strings.HasPrefix(line, "HEAD ") && current != nil:
			head, parseErr := swarmcheck.ParseCommitID(
				strings.TrimSpace(strings.TrimPrefix(line, "HEAD ")),
			)
			if parseErr != nil {
				return nil, parseErr
			}
			current.Head = head
		case line == "detached" && current != nil:
			current.Detached = true
		}
	}
	appendCurrent()
	return resultMap, nil
}

func validDiffStatus(value string) bool {
	switch value {
	case "A", "D", "M", "T", "U", "X":
		return true
	default:
		return false
	}
}

func (inspector *Inspector) git(ctx context.Context, args ...string) (runner.Result, error) {
	return inspector.gitWithEnv(ctx, nil, args...)
}

func (inspector *Inspector) gitWithEnv(
	ctx context.Context,
	environment []string,
	args ...string,
) (runner.Result, error) {
	result, err := inspector.gitResultWithEnv(ctx, environment, args...)
	if err != nil {
		return runner.Result{}, err
	}
	if result.ExitCode != 0 {
		return runner.Result{}, invalid(fmt.Sprintf(
			"git command failed: %s",
			strings.TrimSpace(string(result.Stderr)),
		))
	}
	return result, nil
}

func (inspector *Inspector) gitResultWithEnv(
	ctx context.Context,
	environment []string,
	args ...string,
) (runner.Result, error) {
	gitArgs := []string{
		"--no-replace-objects",
		"--no-pager",
		"--literal-pathspecs",
		"-c", "diff.renames=false",
		"-c", "diff.external=",
		"-c", "diff.submodule=short",
		"-c", "core.quotePath=false",
	}
	gitArgs = append(gitArgs, args...)
	environment, err := sanitizedGitEnvironment(environment)
	if err != nil {
		return runner.Result{}, err
	}
	result, err := inspector.Runner.Run(ctx, runner.RunCmdOpts{
		Name:              gitExecutable,
		Args:              gitArgs,
		Dir:               inspector.Root,
		Env:               environment,
		ReplaceEnv:        true,
		Timeout:           gitCommandTimeout,
		CaptureLimitBytes: maxGitOutputBytes,
	})
	if err != nil {
		return runner.Result{}, errs.Wrap(errs.KindInternal, err)
	}
	if len(result.Stdout) > maxGitOutputBytes || len(result.Stderr) > maxGitOutputBytes {
		return runner.Result{}, invalid("git output exceeds bounded metadata limit")
	}
	return result, nil
}

func swarmcheckSignatureOK(
	value string,
) bool {
	return value == "G" || value == "U" || value == "Y"
}
