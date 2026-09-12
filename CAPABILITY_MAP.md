# Kobwnewe production capability map

Owner approved 2026-09-10. This bounded initiative builds on the completed
manual-QA floor, not the entire MVP roadmap. Product authority remains in
`docs/mvp.md` and its named mirrors.

| Module id | Responsibility | Depends on |
| --- | --- | --- |
| upgrade-safety | Safe native Controller update and Controller-owned Agent replacement; recoverable drain, activation and rollback | Existing Tasks and local Agent lifecycle |
| router-template | Full Caddyfile templating with declared hosts and stable upstreams, validation and retained primary address | Existing HTTP router and Routes |
| setup-scripts | Explicit isolated execution and ordered hook prerequisites without a permanent initializer Service | Existing Script source authority and release hooks |
| kobwnewe-hosting | Reproducible deployment bundle, minimum live Service visibility and application journeys | router-template, setup-scripts |
| recovery | Release-to-release qualification, CI and original-target backup/restore for actual persistent sources | upgrade-safety, kobwnewe-hosting |
| production-cutover | Rehearsed migration, ingress switch and post-switch write/rollback handling | recovery |

Build order: upgrade-safety → router-template → setup-scripts →
kobwnewe-hosting → recovery → production-cutover. An unavailable QA resource
does not block independent implementation: record the deferred proof and return
to it at qualification. Known safety/correctness defects are not QA-resource
exceptions.

Execution: `tasks/plan.md`, `tasks/todo.md`. Specs are named
`SPEC-<module-id>.md` and added before the module's first behavior change.

Delivery to main follows [the execution policy](docs/agents.md#execution-policy):
the primary owns integration and may use bounded Sol/xhigh implementers. The owner
separately approved instruction alignment on 2026-09-12; other cleanup remains
outside this initiative. Live repair remains limited to disposable QA `10.25.0.2`. Production
target selection, production writes, provider DNS/ingress and host networking
changes require explicit operational approval. Existing IPAM, native Controller
topology, Component SDK isolation and external image building are retained.
