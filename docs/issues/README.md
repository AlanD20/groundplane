# Deferred issues

Group related non-blocking findings by feature. Create a separate file only
when a finding needs its own evidence and acceptance conditions. Follow the
[documentation rules](../README.md) when resolving or removing records.
Do not use an issue record to waive a build, security, data-safety or runtime blocker.

The current deployment-first deferral and complete frozen finding inventory are
tracked in [Deferred architecture cleanup](deferred-architecture-cleanup.md).
Current storage, Script, Blueprint, visibility and verification gaps are grouped
in [Runtime qualification](runtime-qualification.md). Resolved and superseded
per-fix journals have been removed; their relevant evidence is feature-routed.
The remaining executable temporary-path mismatch is tracked in
[Tooling alignment](documentation-tooling.md).

Use this form:

```markdown
# <issue title>

- Owner: <person or role>
- Severity: high | medium | low
- MVP-required: yes | no
- Evidence: <commit, tree, test, log, or reproduction>
- Acceptance: <observable condition that closes the issue>
```

Resolve every open issue marked `MVP-required: yes` before declaring the MVP
goal complete. A user-approved deferral must be explicit in the issue; it does
not waive defects that become demonstrable hosting blockers.
