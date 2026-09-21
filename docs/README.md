# Documentation

## Operators

Start with [installation and upgrades](deployment.md), then choose the feature
you need below. [Product scope](mvp.md) explains the resource model and trust
boundary. [Capability status](capabilities.md) distinguishes available
implementation from unfinished or unqualified behavior.

[CLI and API usage](api-cli.md) explains scope, request replay and asynchronous
operations. [Blueprint reference](blueprint.md) explains authored configuration.
The [changelog](../CHANGELOG.md) records release changes, not production readiness.

## Features

| Operator question | Guide |
| --- | --- |
| How do I manage the host, Controller and Agent? | [Platform](features/platform.md) |
| How are applications grouped, renamed and removed? | [Hierarchy](features/hierarchy.md) |
| How do Console actions relate to the CLI and API? | [Console and API](features/console-and-api.md) |
| How do networking, DNS and ingress work? | [Components and routing](features/components.md) |
| How do I customize Caddy routing? | [Router templates](features/router-template.md) |
| How do I deploy, stop or roll back a Service? | [Services and Releases](features/services-and-releases.md) |
| How do I validate and apply desired state? | [Blueprints](features/blueprints.md) |
| How do I share a database, cache or custom container? | [Backing services and Attaches](features/backing-services.md) |
| How do I supply persistent storage and configuration? | [Volumes and Entries](features/storage-and-entries.md) |
| How do I store credentials and select object storage? | [Secrets and Connectors](features/secrets-and-connectors.md) |
| How do setup and migration hooks run? | [Scripts](features/setup-scripts.md) |
| How do I follow work, retry, abort or read logs? | [Tasks and logs](features/tasks-and-logs.md) |
| What are the backup and restore limits? | [Backups](features/backups.md) |
| How are GitHub Runners isolated? | [Runners](features/runners.md) |
| What must an update preserve? | [Update safety](features/upgrade-safety.md) |

## Maintainers

- [Architecture](architecture.md): process and module ownership, data flow and code entry points.
- [Technical decisions](decisions/README.md): reasons, boundaries and consequences.
- [Coding standards](standards.md): rules that are not obvious from the implementation.
- [Delivery](delivery.md): contribution, testing and release expectations.
- [Releasing](releasing.md): version preparation, artifacts, publication and Pages.
- [Agent workflow](agents.md): task authority, bounded delegation and local checkpoints.
- [QA matrix](qa-matrix.md): behavioral cases and independent expected outcomes.
- [Qualification records](acceptance.md#historical-evidence-register) and [open limitations](issues/runtime-qualification.md):
  what earlier runs established and what still needs proof.

Requirements and evidence are different. Accepted design is not necessarily
implemented; implementation is not necessarily qualified. Missing code does not
silently remove a requirement.

## Documentation rules

Every document has an audience and answers a concrete question.

Operator guides explain available actions, inputs, consequences, failure handling
and limitations. Include a small example when it helps someone perform the task.
Mark accepted-but-unavailable behavior where the operator would otherwise try it.

Architecture documentation explains who owns a decision, why that boundary exists,
and which consequences matter. Link to the owning code. Do not translate functions
into prose or copy structs, source trees, command inventories or implemented wire
schemas. The code, generated OpenAPI, protobuf and command help own those details.
An accepted protocol not yet implemented may retain its missing contract in one
clearly marked design reference; do not present it as a usable feature.

Use ordinary technical language. Name the actor, action and consequence. Define
necessary terms once. Delete filler, unexplained labels, repeated constraints and
claims such as “production-ready” without evidence. No mandatory six-section
template, research-paper format or document per fix.

Keep each requirement in one owner:
product-wide rules in `mvp.md`, feature behavior in its feature guide, authored
syntax in `blueprint.md`, surface conventions in `api-cli.md`, module boundaries
in `architecture.md`. Link instead of restating. ADRs explain decisions; they do
not form a competing product specification.

Before retiring a document, retain its still-valid requirements, rationale and
unresolved limitations in the appropriate owner. Repair incoming links in the
same change. Delete the superseded file; Git is the archive. Do not keep a second
Markdown archive, compatibility document or stale “next steps” list.

Agent checkpoints, investigation transcripts, work plans, private host details
and raw QA artifacts stay in ignored local storage. Public documentation must be
usable without those files. A dated incident describes that run, not current host
state or permission for future actions. Update the current checkpoint instead of
appending an endless journal.

Preserve the behavioral QA matrix and honest qualification limits. Tests need
a concrete behavioral reason under [testing policy](delivery.md#testing-policy);
documentation edits do not call for static-text tests or a build.
