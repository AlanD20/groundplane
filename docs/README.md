# Groundplane documentation

Documentation describes the current product, its requirements and its design.
Git preserves previous versions. Start with the document that owns the question.

## Document owners

| Document | Purpose |
| --- | --- |
| [mvp.md](mvp.md) | Product scope, shared behavior, domain terms and acceptance outcomes |
| [Feature documents](#features) | One entry point for a feature's requirements, design and acceptance |
| [api-cli.md](api-cli.md) | Operator actions, CLI commands and REST surfaces |
| [blueprint.md](blueprint.md) | Desired-state grammar and validation |
| [architecture.md](architecture.md) | Shared process, module and dependency boundaries; wire generation |
| [standards.md](standards.md) | Enforceable coding rules |
| [agents.md](agents.md) and [delivery.md](delivery.md) | Repository workflow, verification and delivery |
| [deployment.md](deployment.md) and [acceptance.md](acceptance.md) | Deployment and qualification procedures |
| [qa-matrix.md](qa-matrix.md) | Product cases, independent expected outcomes, coverage gaps and links to execution evidence |
| [head.md](head.md) | Current work, authority limits, blockers and next actions |
| [capabilities.md](capabilities.md) | Product-wide implementation and qualification gaps |

Read relevant sections, not every linked document. Read `head.md` in full after
context recovery. Requirements describe what must be true; implementation and
qualification status describe what has been built and proved. Neither a missing
implementation nor an old test result changes a requirement.

## Features

| Feature | Document |
| --- | --- |
| Host health, bootstrap, Controller and Agent settings | [Platform runtime](features/platform.md) |
| Resource ownership, names and aggregate deletion | [Hierarchy](features/hierarchy.md) |
| Operator surfaces and production SPA delivery | [Console and API](features/console-and-api.md) |
| Managed Components, DNS, Routes and Zone membership | [Components and routing](features/components.md) |
| Durable work, Activity, cancellation and transient output | [Tasks and logs](features/tasks-and-logs.md) |
| Persistent storage and exposed configuration values | [Volumes and Entries](features/storage-and-entries.md) |
| Revisioned desired-state editing and reconciliation | [Blueprints](features/blueprints.md) |
| Workload lifecycle, Deploy, Rollback and recovery | [Services and Releases](features/services-and-releases.md) |
| Policy, artifacts, source recovery and retention | [Backups](features/backups.md) |
| Shared PostgreSQL/Valkey instances and consumer credentials | [Backing services and Attaches](features/backing-services.md) |
| Reusable values and S3 credential configuration | [Secrets and Connectors](features/secrets-and-connectors.md) |
| Isolated GitHub Actions execution | [Runners](features/runners.md) |
| Controller and Agent updates | [Safe updates](features/upgrade-safety.md) |
| Complete operator-authored Caddyfile | [Router templates](features/router-template.md) |
| Ordered setup hooks with explicit resources | [Script execution](features/setup-scripts.md) |

Feature entrypoints route detailed runtime, persistence and wire contracts when
those details would obscure the feature overview. The retained
[technical decisions](decisions/README.md) are current design references, not a
chronological reading list. Read only the sections needed for the task.

## Feature document

Use one living `docs/features/<feature>.md` for each coherent feature. A small
change belongs in its existing feature document, not a new file per task or fix.
Use these sections when relevant:

1. **Purpose and scope.** Who needs the feature, the outcome, and what it excludes.
2. **Functional requirements.** Operator behavior, inputs, outputs, state changes
   and failure behavior. Link to the API and Blueprint definitions.
3. **Non-functional requirements.** Relevant security, reliability, performance,
   resource and compatibility constraints. State measurable limits when they are
   required; do not invent targets to fill a template.
4. **Technical design.** Owning modules, interfaces, data flow, state transitions
   and dependencies. Explain important choices and link to the code and schemas.
5. **Acceptance.** Observable conditions and the tests or operator checks that
   prove them. Map behavioral tests to stable cases in [the QA matrix](qa-matrix.md)
   and link per-case evidence; do not paste execution transcripts. Build success
   is a prerequisite, not feature acceptance.
6. **Current status.** Distinguish agreed requirements, implemented behavior and
   completed qualification. State remaining gaps and link to their evidence.

A feature document should explain the behavior and why its design meets the
requirements. Put shared rules in their owning document and link to them.
Generated schemas, manifests and code remain the sources for mechanical details.

## Plain language

Name the actor, action, object and failure condition. Use one term for one
concept, and define unfamiliar terms when they first matter. Preserve exact
code and API names; explain them rather than inventing new labels.
Use normal spacing, short sentences and headings that describe their content.
Keep the reason for a non-obvious constraint beside that constraint.
Remove filler, repeated rules, unexplained abbreviations and claims such as
“robust” or “production-ready” without a stated condition and evidence.
Clarity and complete meaning matter more than an arbitrary word limit.

## Maintenance and deletion

- Update the owning document when behavior or design changes. Update only
  affected references and dependent contracts; do not copy the same rule into
  several documents.
- Record a design choice in the feature document. Use a separate ADR when a
  shared or expensive-to-reverse decision needs its own rationale and alternatives.
  An ordinary commit does not require a new ADR or acceptance report.
- Keep one current work checkpoint. Replace completed next actions instead of
  appending a journal. Group related open issues; remove resolved task lists.
- Retain evidence needed for unfinished qualification, unresolved incidents or
  a claim still used by the project. It is dated evidence, not a current command
  or permission. Keep private logs and credentials out of tracked documentation.
- Before deleting a document, check its requirements, rationale, unresolved
  work and incoming references. Move anything still needed to its current owner,
  then remove the old file and repair references in the same commit.
- Fully superseded ADRs, plans and reports may be deleted after that check.
  Record the replacement in the commit; recover history from Git when needed.
  Do not build another Markdown archive of obsolete instructions.
- A missing implementation is not a stale requirement. Preserve uncommitted
  work and failed evidence. Ask the owner when a conflict needs a product decision.

## Moving an existing contract

Migrate one feature or related document group at a time. Record the source and
destination, preserve requirement meaning and qualification limits, check
references, and land the slice before starting another independent migration.
A readability change does not authorize new behavior or code restructuring.

`mvp.md`, `api-cli.md`, `blueprint.md` and `architecture.md` retain their shared
authority. A feature document owns its feature-specific detail and routes exact
technical contracts. Moving a requirement means updating its old home and readers
in the same slice, not creating competing copies or changing its approval status.
