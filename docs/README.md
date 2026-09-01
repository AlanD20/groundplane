# Groundplane documentation

Use this directory as a routed contract, not a collection of overlapping
summaries. Each decision has one authoritative home.

| Document | Read when |
| --- | --- |
| `mvp.md` | Product behavior, domain model, scope, or locked decisions |
| `api-cli.md` | Console actions, CLI commands, REST endpoints, or the 1:1 rule |
| `blueprint.md` | Desired state, Compose fields, YAML, ids, or validation |
| `architecture.md` | Module layout, process boundaries, extension seams, or frontend stack |
| `standards.md` | Writing or reviewing Go code and CI checks |
| `agents.md` | Planning repository changes or deciding which contract must move |
| `delivery.md` | Commits, review, quality gates, or release preparation |
| `capabilities.md` | MVP implementation status, contract gaps, and vertical delivery order |
| `decisions/` | Changing an architectural choice that has recorded rationale |

`mvp.md` wins product conflicts. Update every mirror in the same change:
the Console store, CLI tree, REST resource map, Blueprint grammar, and
architecture documentation.
