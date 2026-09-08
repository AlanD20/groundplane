# Deferred issues

Create one Markdown file per non-blocking finding. Do not record a build,
security, data-safety, or runtime blocker here instead of fixing it.

The current deployment-first deferral and complete frozen finding inventory are
tracked in [Deferred architecture cleanup](deferred-architecture-cleanup.md).

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
