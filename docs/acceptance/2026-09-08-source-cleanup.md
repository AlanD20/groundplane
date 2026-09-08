# Main-only source cleanup — 2026-09-08

The owner explicitly requested removal of every non-main branch and secondary
worktree, after deployment-management checks and needed-source integration.

## Final state

- One local branch: `main`.
- One registered worktree: the repository root on `main`.
- Removed 357 non-main local branches and 117 registered secondary worktrees.
- Removed five additional stale standalone Groundplane checkouts under ignored
  temporary paths, after separately archiving their complete Git directories.
- Remote refs and the remote repository were not mutated.
- The existing stash and ordinary ignored QA inputs outside removed checkouts
  were retained.

## Integration disposition

The six completed September8 code branches already matched `main` exactly on
their own changed files: app Blueprint store, Compose invocation extraction,
adapter test ownership, CoreDNS refresh, Controller SDK fixtures, and registered
Component equality tests. No duplicate cherry-picks were needed.

The three existing root-worktree fixture changes were preserved in
`4f106555d`. Blueprintrelease and Volume-removal package race tests passed;
the app test package compiled with no tests executed. No backup/restore tests
or operations were run.

A useful uncommitted duplicate-Script-body test from the old Script worktree
was integrated in `abed1350d`, adding a positive control before the rejection
assertion. Its focused Agent race test passed. No production behavior changed.

The narrow Volume authored-metadata correction is `23084cefa`; its live limits
are recorded in [management checks](2026-09-08-management-checks.md).

Historical snapshots and unfinished experiments were **archived, not
bulk-merged**. Earlier history was rewritten, so lack of ancestry was not treated
as proof that old code should be merged. The paused Service-module extraction
remains incomplete; it was not transplanted into the working deployment.
An old managed-source-on-candidate regression still fails the current
`historical_apply` ownership guard and was preserved as unlanded evidence.
The standalone acknowledgement change is already represented by the current
named Blueprint-root-condition helper and its tests. Removed host-RPi tooling,
superseded preflight stores/helpers, generated snapshots, and explicitly excluded
backup/restore work were not reintroduced.

## Recovery archive

`.tmp/branch-worktree-archive-20260908/` is ignored, mode0700, approximately
5.1GiB. It is not a branch or worktree and must not be published; ignored private
QA inputs may be present.

- `all-refs.bundle` preserves complete Git history, refs, stash, and every linked
  worktree HEAD, including detached heads. Git bundle verification passed.
- `worktrees.tar.gz` preserves complete linked-worktree contents, including
  tracked, untracked, and ignored files. Full tar read and checksum passed.
- `standalone-checkouts.tar.zst` preserves all five additional checkouts,
  including `.git`; full read, checksum, and comparison with the source passed.
- `patches/` preserves staged, unstaged, and combined binary diffs independently.
- `inventory.json`, manifests, checksums, `branch-deletion.txt`, and
  `cleanup-result.json` retain exact targets and receipts.
- `README.md` provides recovery guidance.

Before removal, every linked worktree's HEAD, branch, staged patch, unstaged
patch, and status matched the archive. All branch names and commits matched the
frozen inventory. No other agent was running. Cleanup used those exact targets,
never a broad recursive deletion of the repository or temporary-state root.

The initial inventory and per-file source comparison remain under
`.tmp/qa-management-20260908/`. They distinguish byte-identical, already-applied,
forward-applicable, divergent, and absent old paths; applicability alone was not
treated as correctness or authorization to merge.
