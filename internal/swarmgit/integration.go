package swarmgit

import (
	"context"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/swarmcheck"
)

const syntheticCommitDate = "2000-01-01T00:00:00Z"

func (inspector *Inspector) expectedPrefixTrees(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) ([]swarmcheck.TreeID, error) {
	return inspector.expectedPrefixTreesThrough(ctx, manifest, len(manifest.Integration))
}

func (inspector *Inspector) expectedPrefixTreesThrough(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	count int,
) ([]swarmcheck.TreeID, error) {
	if count < 0 || count > len(manifest.Integration) {
		return nil, invalid("integration prefix count is invalid")
	}
	environment := syntheticCommitEnvironment()
	allSnapshots := make(map[swarmcheck.CommitID]swarmcheck.Snapshot, len(manifest.Snapshots))
	for _, snapshot := range manifest.Snapshots {
		allSnapshots[snapshot.Commit] = snapshot
	}
	snapshots := make(map[swarmcheck.Lane]swarmcheck.Snapshot, len(manifest.Active))
	for _, selected := range manifest.Active {
		snapshot, exists := allSnapshots[selected.Snapshot]
		if !exists {
			return nil, invalid("active integration snapshot is absent")
		}
		snapshots[selected.Lane] = snapshot
	}
	approved := make(map[swarmcheck.CommitID]struct{}, len(manifest.Reviews))
	for _, review := range manifest.Reviews {
		if review.Approved {
			approved[review.Snapshot] = struct{}{}
		}
	}
	currentCommit := manifest.Base.Commit
	prefixTrees := make([]swarmcheck.TreeID, 0, count)
	for index, lane := range manifest.Integration[:count] {
		snapshot, ok := snapshots[lane]
		_, isApproved := approved[snapshot.Commit]
		if !ok || !isApproved {
			return nil, invalid(
				fmt.Sprintf("integration lane %s lacks its approved snapshot", lane),
			)
		}
		merged, err := inspector.gitWithEnv(
			ctx,
			environment,
			"merge-tree",
			"--write-tree",
			string(currentCommit),
			string(snapshot.Commit),
		)
		if err != nil {
			return nil, err
		}
		tree, ok := exactTreeID(merged.Stdout)
		if !ok {
			return nil, invalid("git merge-tree returned malformed or conflicting output")
		}
		prefixTrees = append(prefixTrees, tree)
		synthetic, err := inspector.gitWithEnv(
			ctx,
			environment,
			"-c",
			"user.name=Groundplane Swarm Checker",
			"-c",
			"user.email=swarm-check@localhost",
			"-c",
			"commit.gpgSign=false",
			"commit-tree",
			string(tree),
			"-p",
			string(currentCommit),
			"-m",
			fmt.Sprintf("groundplane swarm %s prefix %d", manifest.Wave, index+1),
		)
		if err != nil {
			return nil, err
		}
		currentCommit, ok = exactCommitID(synthetic.Stdout)
		if !ok {
			return nil, invalid("git commit-tree returned malformed output")
		}
	}
	if count != 0 && len(prefixTrees) == 0 {
		return nil, invalid("integration order has no snapshots")
	}
	return prefixTrees, nil
}

func syntheticCommitEnvironment() []string {
	return []string{
		"GIT_AUTHOR_NAME=Groundplane Swarm Checker",
		"GIT_AUTHOR_EMAIL=swarm-check@localhost",
		"GIT_AUTHOR_DATE=" + syntheticCommitDate,
		"GIT_COMMITTER_NAME=Groundplane Swarm Checker",
		"GIT_COMMITTER_EMAIL=swarm-check@localhost",
		"GIT_COMMITTER_DATE=" + syntheticCommitDate,
	}
}

func (inspector *Inspector) validateStoredIntegrationConflicts(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	snapshots := make(map[swarmcheck.CommitID]swarmcheck.Snapshot, len(manifest.Snapshots))
	for _, snapshot := range manifest.Snapshots {
		snapshots[snapshot.Commit] = snapshot
	}
	for _, conflict := range manifest.Conflicts {
		current := manifest.Base.Commit
		for index, commit := range conflict.PrefixSnapshots {
			snapshot := snapshots[commit]
			merged, err := inspector.gitWithEnv(
				ctx,
				syntheticCommitEnvironment(),
				"merge-tree",
				"--write-tree",
				string(current),
				string(snapshot.Commit),
			)
			if err != nil {
				return err
			}
			tree, ok := exactTreeID(merged.Stdout)
			if !ok {
				return invalid("integration conflict prefix produced malformed merge output")
			}
			synthetic, err := inspector.gitWithEnv(
				ctx,
				syntheticCommitEnvironment(),
				"-c",
				"user.name=Groundplane Swarm Checker",
				"-c",
				"user.email=swarm-check@localhost",
				"-c",
				"commit.gpgSign=false",
				"commit-tree",
				string(tree),
				"-p",
				string(current),
				"-m",
				fmt.Sprintf("groundplane swarm %s prefix %d", manifest.Wave, index+1),
			)
			if err != nil {
				return err
			}
			current, ok = exactCommitID(synthetic.Stdout)
			if !ok {
				return invalid("integration conflict prefix commit is malformed")
			}
		}
		if current != conflict.PrefixCommit {
			return invalid("integration conflict does not bind the recomputed prefix commit")
		}
		result, err := inspector.gitResultWithEnv(
			ctx,
			syntheticCommitEnvironment(),
			"merge-tree",
			"--write-tree",
			string(current),
			string(conflict.Snapshot),
		)
		if err != nil {
			return err
		}
		if result.ExitCode != 1 {
			return invalid("integration conflict cannot be reproduced from its prefix and snapshot")
		}
	}
	return nil
}

func validateAppliedTrees(manifest swarmcheck.Manifest, prefixTrees []swarmcheck.TreeID) error {
	if len(manifest.Applied) > len(prefixTrees) {
		return invalid("applied receipt count exceeds computed prefixes")
	}
	for index, receipt := range manifest.Applied {
		if receipt.PrimaryIndexTree != prefixTrees[index] {
			return invalid(
				fmt.Sprintf("applied receipt %d does not match the computed prefix tree", index),
			)
		}
	}
	return nil
}

func (inspector *Inspector) validatePhaseState(
	ctx context.Context,
	manifest swarmcheck.Manifest,
	state Inspection,
) error {
	switch manifest.Phase {
	case swarmcheck.PhaseDispatch, swarmcheck.PhaseWriting, swarmcheck.PhaseReview:
		if state.Head != manifest.Base.Commit || state.IndexTree != manifest.Base.Tree ||
			!state.Clean {
			return invalid(
				fmt.Sprintf(
					"phase %s requires primary HEAD and index tree at base",
					manifest.Phase,
				),
			)
		}
	case swarmcheck.PhaseIntegrate:
		if err := inspector.validateIntegrationDrift(ctx, manifest); err != nil {
			return err
		}
		expected := manifest.Base.Tree
		if len(manifest.Applied) != 0 {
			var prefixTrees []swarmcheck.TreeID
			var err error
			if swarmcheck.HasPendingConflictRemediation(manifest) {
				prefixTrees, err = inspector.expectedPrefixTreesThrough(
					ctx,
					manifest,
					len(manifest.Applied),
				)
			} else {
				prefixTrees, err = inspector.expectedPrefixTrees(ctx, manifest)
			}
			if err != nil {
				return err
			}
			if err := validateAppliedTrees(manifest, prefixTrees); err != nil {
				return err
			}
			expected = prefixTrees[len(manifest.Applied)-1]
		}
		if manifest.Delivery == nil &&
			(state.Head != manifest.Base.Commit || state.IndexTree != expected) {
			return invalid("integrate phase index tree does not match the applied receipt prefix")
		}
		if manifest.Delivery != nil &&
			(state.Head != manifest.Delivery.Commit || state.IndexTree != manifest.Delivery.Tree ||
				manifest.Delivery.Tree != expected || !state.Clean) {
			return invalid(
				"gate-ready integrate phase must be clean at the signed delivery candidate",
			)
		}
	case swarmcheck.PhaseComplete:
		prefixTrees, err := inspector.expectedPrefixTrees(ctx, manifest)
		if err != nil {
			return err
		}
		if err := validateAppliedTrees(manifest, prefixTrees); err != nil {
			return err
		}
		expectedFinalTree := prefixTrees[len(prefixTrees)-1]
		if manifest.Delivery == nil || state.Head != manifest.Delivery.Commit ||
			state.IndexTree != expectedFinalTree || manifest.Delivery.Tree != expectedFinalTree || !state.Clean {
			return invalid("complete phase tree does not match the signed delivery candidate")
		}
		deliveryRef, err := inspector.resolve(ctx, string(manifest.TargetRef))
		if err != nil || deliveryRef != manifest.Delivery.Commit {
			return invalid("complete phase target ref is not the signed delivery commit")
		}
	}
	return nil
}

func (inspector *Inspector) validateIntegrationDrift(
	ctx context.Context,
	manifest swarmcheck.Manifest,
) error {
	status, err := inspector.status(ctx, true)
	if err != nil {
		return err
	}
	return validatePrimaryStatus(status, manifest.Wave, true)
}

func validatePrimaryStatus(
	status statusEvidence,
	wave swarmcheck.WaveID,
	allowStaged bool,
) error {
	if !allowStaged && status.Staged || status.Unstaged || status.Unmerged || status.Sparse ||
		status.AssumeUnchanged {
		return invalid(
			"primary state has forbidden staged, unstaged, unmerged, sparse, or assume-unchanged drift",
		)
	}
	waveRoot := swarmcheck.RepoPath(".tmp/swarm/" + string(wave))
	for _, changed := range status.Paths {
		switch changed.Status {
		case statusUntracked:
			return invalid(
				fmt.Sprintf("primary state has untracked drift: %s", changed.Path),
			)
		case statusIgnored:
			if changed.Path != waveRoot &&
				!strings.HasPrefix(string(changed.Path), string(waveRoot)+"/") {
				return invalid(
					fmt.Sprintf("primary state has non-wave ignored drift: %s", changed.Path),
				)
			}
		}
	}
	return nil
}

func exactCommitID(output []byte) (swarmcheck.CommitID, bool) {
	return exactParsedID(output, swarmcheck.ParseCommitID)
}

func exactTreeID(output []byte) (swarmcheck.TreeID, bool) {
	return exactParsedID(output, swarmcheck.ParseTreeID)
}

func exactParsedID[T ~string](output []byte, parse func(string) (T, error)) (T, bool) {
	value := strings.TrimSuffix(string(output), "\n")
	if strings.ContainsAny(value, " \t\r\n") {
		var zero T
		return zero, false
	}
	parsed, err := parse(value)
	return parsed, err == nil
}
