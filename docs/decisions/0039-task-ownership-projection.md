# ADR 0039: Immutable Task ownership projection

Status: accepted

## Context

The MVP requires platform, tenant, and Environment views over one exact Task
record set. Durable and public Task records currently expose execution target
but not immutable workspace ownership or journal timestamps. Deriving scope by
walking a mutable target hierarchy at read time would make historical Tasks
move or disappear after renames and deletion, and would not support exact
fixed-revision pagination.

## Decision

### Immutable owner

Every Task freezes one owner at creation. Ownership comes from the
operator-visible capability that initiated the work, never from `target`, the
executor, or a later hierarchy lookup:

- `workspace_type` is exactly `platform` or `tenant`;
- `tenant_id` is required exactly when `workspace_type` is `tenant` and absent
  for `platform`;
- `project_id` is present when the initiating capability is Project-owned or
  below;
- `environment_id` is present when the initiating capability is
  Environment-owned or below, and then `project_id` is also required.

The referenced Tenant, Project, and Environment must be one valid hierarchy at
Task creation. A tenant Project freezes `workspace_type: tenant` and its
`tenant_id`. A backing Project has no Tenant and freezes `workspace_type:
platform`. Owner fields never change as the Task runs. A retry is a new Task
that copies the complete owner tuple from the immediately retried attempt; an
abort changes the addressed Task and creates no second owner.

Operations map to owners as follows:

- Environment creation/deletion and every Environment descendant operation use
  the Environment owner. This includes Blueprints, Services, Release Groups,
  Attaches, Zones, Routes, Volumes, Entries, Scripts, Connectors, backups, and
  Environment Components. Backing-service operations therefore carry the
  backing Project and its `main` Environment in the platform workspace.
- A tenant Project deletion, Project Secret removal, or Project-owned Runner
  lifecycle uses the tenant workspace with `tenant_id` and `project_id`, but no
  `environment_id`.
- Tenant deletion and a Tenant-owned Runner lifecycle use the tenant workspace
  with only `tenant_id`.
- Agent lifecycle, platform Component actions, platform Secret removal, and
  Controller-owned platform maintenance use the platform workspace with no
  Tenant, Project, or Environment id.

A cascade retains the initiating operation's owner even when its steps touch
descendants. Scheduled Environment work has the Environment owner just like an
operator-started run.

### Journal fields and actor

The public and durable journal projection contains Controller-authored UTC
`created_at`, `updated_at`, nullable `started_at`, and nullable `finished_at`.
Creation sets `created_at` and `updated_at` equally. Every durable public Task
state or step-summary change advances `updated_at`; `started_at` is set once on
the first running transition and `finished_at` once on the terminal transition.

The single-operator token MVP has no user directory or per-token display
identity. `actor` is therefore the closed value `operator` for work initiated
by an authenticated API/Console/CLI request and `system` for scheduler,
reconciliation, recovery, and other Controller-initiated work. It stores no
token, token digest, username, or role. A retry records the actor that requested
that retry; ownership still copies from its source Task.

### Fixed-revision indexes and pages

Task creation atomically writes the primary plus exactly one workspace index:

```text
/v1/indexes/tasks/by-workspace/platform/<task-id>
/v1/indexes/tasks/by-workspace/tenant/<tenant-id>/<task-id>
```

An Environment-owned Task also writes:

```text
/v1/indexes/tasks/by-environment/<environment-id>/<task-id>
```

Each index value is the exact Task id. The Task ULID is the immutable
creation-order key, so global primary and scoped index pages all use ascending
Task-id order without a second time key. The Task-pruning transaction deletes
both owner indexes with the public primary. Missing, extra, malformed, or
owner-mismatched indexes are corruption; readers never repair or skip them.

`GET /tasks` and `GET /activity` accept `limit`, `cursor`, and at most one of:

- `environment=<env-id>`, read directly from the Environment index;
- `project=<prj-id>`, read through a bounded fixed-revision primary scan that
  matches immutable `project_id`; or
- `workspace=platform|<tenant-id>`, read directly from the selected workspace
  index.

No scope returns the global primary collection. The API accepts stable ids;
the CLI resolves its Tenant, Project, and Environment hierarchy labels and the
non-`platform` `--workspace` Tenant label to stable ids before calling it.
Supplying multiple scopes or a scope inconsistent with a cursor is
`validation.failed`.

The first page fixes one etcd revision. It reads the chosen primary/index range
and all referenced Task primaries at that revision. The cursor binds the
logical `tasks` collection, exact scope, limit, ascending order, revision, and
last Task id. Later pages use the same revision; compaction returns
`cursor.expired` and never restarts. `/activity` is an exact alias: identical
filters, records, order, page boundaries, errors, and cursor identity. A cursor
issued by either route is valid on the other.

The Console's Platform Activity page starts from the global journal and may
narrow it by Tenant, Project, or Environment. Tenant Activity starts from that
Tenant's workspace journal and may narrow it by Project or Environment. These
filters never broaden the surface's starting visibility. The Tasks section on
Platform Components remains the `workspace=platform` journal. No surface
narrows by `target`, Component kind, or operation type. The MVP has no Task
index or list filter by execution target or Component; `target` may be rendered
or used for navigation, but it never owns or selects a journal record.

### Clean start

The MVP performs no backfill, target inference, dual read, or
compatibility index. Existing development Task records without the complete
owner, actor, timestamp, and index contract are incompatible; startup must fail
closed or the development store must be cleared before the new schema is used.

## Consequences

Historical Tasks remain in the workspace, Project, and Environment where their
initiating operation occurred even after labels change or owners are deleted.
Global, workspace, Project, and Environment journals paginate at one fixed
revision, and Task and Activity cannot drift into separate record sets. The
cost is two small immutable indexes for Environment-owned Tasks, a bounded
primary scan for Project pages, and a deliberate clean start for pre-contract
development data.
