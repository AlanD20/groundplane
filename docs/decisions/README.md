# Technical decisions

Start with the [feature index](../README.md#features) or the relevant shared
contract. They route the technical sections needed for a task. Do not read every
ADR by number or use an old implementation sequence as the current work plan.

Retained decisions explain current seams, failure behavior, exact protocols,
storage limits and their rationale. Feature entrypoints keep the operator
requirements and acceptance readable; a link to detailed design is intentional,
not unfinished migration. Update the owning section when its contract changes.
Git retains superseded decisions removed after their valid content was migrated.

Approval and implementation are distinct:

- **Accepted** defines an agreed design within the shared product contracts.
  It does not prove implementation, passing tests or deployment.
- [ADR 0013](0013-etcd-record-and-index-layout.md) has approved storage subsets
  but remains Proposed as a whole. Follow its named approvals and later accepted
  resource-specific contracts, not unresolved recommendations as if approved.
- [ADR 0055](0055-revision-bound-network-observation-and-backing-blueprint-scope.md)
  is Deferred post-MVP. It preserves future constraints, not current wire fields,
  schemas or existing-Zone selection authority.
- [ADR 0053](0053-durable-hierarchy-and-backing-facade-deletion.md#current-authority-limit)
  has an unresolved backing-deletion surface conflict. Its generic hierarchy
  engine remains accepted; current Backing Service Destroy remains runtime-only.

For actual progress, use [capabilities](../capabilities.md),
[current tasks](../../tasks/todo.md) and the [work checkpoint](../head.md).
