# Console UX principles and standards

This is maintainer guidance for designing and reviewing the whole Console.
It defines the target standard for future changes; it does not claim that every
existing screen conforms. The [operator guide](features/console-and-api.md)
describes current behavior. [Product scope](mvp.md) owns resource semantics and
supported actions; this document owns their presentation and interaction rules.

The goal is to help operators find a capability, understand what it affects and
complete their work without losing context. All supported functionality must
remain discoverable. A cleaner appearance alone is not evidence of better UX.

## Principles

| Principle | Required consequence |
| --- | --- |
| Make scope and ownership obvious. | Show which resource is selected and where a value or resource belongs. Explain shared effects before editing. |
| Give capabilities a home and contextual access. | Use one consistent management workflow, reachable from the places that need it. |
| Keep capabilities visible; focus their details. | Named destinations and meaningful summaries remain available while lengthy forms and diagnostic output open on demand. |
| Group around recognizable work. | A destination's name predicts its contents. Independent workflows do not disappear into a generic bucket. |
| Preserve context while working. | Simple edits happen beside the relevant information; deeper work retains scope and a clear return path. |
| Make consequences and state truthful. | Distinguish saving, applying, running and observing. Show failures and incomplete work where the operator acts. |
| Use one interaction language. | Equivalent controls, terms, states and layouts behave consistently across scopes. |

## Scope and navigation

Ownership follows [Hierarchy](features/hierarchy.md), including the separate
Platform-owned Backing Service model. The Tenant → Project → Environment →
Service path is context, not a requirement to repeat the same menus at every
level. Sharing, referencing and inheritance are different concepts: do not imply
automatic inheritance from the hierarchy.

The following examples explain placement, not an exhaustive resource inventory:

| Context | Placement rule |
| --- | --- |
| Platform | Host and shared infrastructure management has a distinct home. Show consumers when a shared change affects them. |
| Tenant | Keep Projects and Tenant-scoped operations identifiable; do not suggest that every Project setting exists at Tenant scope. |
| Project | Keep Environments and Project-owned reusable inputs identifiable. Show which Environment is selected before an Environment action. |
| Environment | Manage its resources here. Show their relationships to Services rather than requiring operators to remember names across screens. |
| Service | Expose the resources used by this workload and its editable relationships: for example, a mount selects an Environment Volume. Link to the Volume's management workflow. |
| Backing Service | Show the shared instance and consuming connections distinctly. Consumer-specific work retains its consumer scope; do not invent an instance-wide capability. |

Separate **scope selection** (which Project, Environment or Service) from
**local navigation** (what to do with that resource). Keep ancestry visible and
switchable. Opening a child workspace replaces the parent's local navigation;
do not stack several levels of horizontal tabs. Local navigation contains only
work within the selected scope. Use breadcrumbs and an explicit parent link to
leave it; do not append unrelated Tenant or Project shortcuts to an Environment
menu. Global collection pages without local destinations use the Platform rail
alone, rather than filling a secondary panel with unrelated resources.

Use consistent names and relative ordering for equivalent destinations. Different
resource types may need different destinations. There is no fixed tab count.
When destinations exceed a usable tab row, use a labeled local navigation list
with visible section headings; a heading need not introduce another page.

Give meaningful destinations stable URLs. Browser Back and return links should
restore the relevant selection and list context. Scope switches must make the
new scope unmistakable and handle unavailable destinations explicitly.

## Grouping and discoverability

A proposed group must pass two questions: can an operator predict what is inside
from its name, and do its contents support a coherent task or resource lifecycle?
“Configuration,” “Connections” and “Settings” are not automatic answers. Name
independent destinations when a bucket combines unrelated work. For example,
public Routes and credentials for a Backing Service have different purposes;
the fact that both involve connectivity is insufficient to merge them.

Keep relevant capability destinations present when empty, with a named creation
or setup action. When an otherwise relevant action is unavailable, explain the
prerequisite or restriction. Respect authorization; visibility is not permission
to expose protected information.

An overview answers: what is this resource, what is happening, and what needs
attention? Summarize important relationships and state with direct actions.
Do not reproduce every form or create a second full navigation grid of cards.

Disclosures may contain lengthy diagnostics, identifiers or optional field
details. They must have specific labels. Core capabilities must not be accessible
only through “Technical details,” “Advanced,” “More,” hover or search. Search and
overflow menus supplement the visible navigation; overflow is appropriate for
secondary row actions, with consistent placement and accessible labels.

## Contextual actions and editing

One capability has one owning workflow. Contextual entry points reuse that
workflow with the correct resource selected; they do not create competing forms
or different semantics. This preserves [Console/CLI/API parity](api-cli.md).

Show a value's owner or source when it affects how the operator can change it.
For a reference, distinguish editing the source from replacing this resource's
reference. State the affected scope before saving a shared change. Follow actual
[Secret resolution and exposure](features/secrets-and-connectors.md); never
label a value “inherited” merely because it came from another scope.

Use inline controls for small independent changes with a clear effect, and a
focused drawer or dialog for a bounded edit. Use a workspace for extended work
with multiple resources or substantial history. Do not turn each toggle into a
separate page or put an entire resource's configuration into one large modal.
Avoid nested modal chains; dependent creation must retain or restore the original
selection and unsaved input.

Display current values before asking operators to edit. Keep validation near the
field and the primary action visible. Make Save/Cancel and unsaved changes
explicit for staged forms. An immediately applied control must communicate that
behavior and report pending, success and failure; do not mix immediate and staged
edits without distinguishing them.

Name actions by their effect. Saving desired configuration is not deploying it;
accepted work is not completed work; completed work is not proof of application
health. Surface relevant pending changes and Task progress next to the affected
resource, with a link to detailed work and a return path. Show unavailable or
stale observations honestly. Destructive actions must identify the target and
data consequences using the owning feature's actual contract.

## Language and shared presentation

Use familiar, specific labels with one meaning throughout the Console. Action
labels use a verb and object when the object is otherwise ambiguous. Avoid
implementation vocabulary in operator instructions and vague section names.

Friendly labels must preserve domain distinctions. “Variables & files” can explain
Entry exposure; “Backing connections” can introduce Attaches. Keep canonical terms
available in contextual help and documentation so operators can connect Console,
CLI and API usage. Connection values, reusable Secrets and exposed variables are
not interchangeable. A Task is not necessarily a deployment.

Compose existing shared controls and design tokens rather than inventing local
variants. Equivalent summaries, editors, status indicators and empty/error states
should share their structure and interaction. [Architecture](architecture.md#console)
owns implementation placement.

Use locally bundled IBM Plex Sans for interface text and IBM Plex Mono for code,
logs and machine-readable values. Shared controls and navigation use 14px text;
secondary text uses 12–13px, section headings 18–20px and page titles 28px.
Use regular, medium and semibold weights for text, controls and headings. Prefer
these shared sizes over tiny local overrides; check wrapping in narrow layouts.

Use persistent labels for resource navigation and important controls. The global
Platform navigation uses a compact vertical icon rail with accessible names and
labels on hover and keyboard focus. In the secondary navigation, pair destination
labels with icons, separate groups with space and rules, and distinguish section
headings from links. Mark the selected destination with shape and weight as well
as color. Support keyboard
operation, visible focus, labeled fields and predictable focus return after an
editor closes. Do not communicate state through color alone or make essential
actions depend on hover.

Long names, errors, Secrets and connection strings must stay inside page and
dialog boundaries at narrow widths. Wrap ordinary text; use bounded scrolling
where code or logs need it. Truncation must provide access to the complete value.
Keep Copy and reveal/hide controls usable without expanding the layout. Do not
reveal sensitive values automatically to improve discoverability.

## Applying and reviewing the standard

Before changing navigation, map the affected capabilities across the whole
hierarchy. Use the product contract, operator guides and implementation together;
an absent UI action does not narrow the product contract. For each capability,
record its owner, consumer contexts, operator task, destination, action entry
points and effect. Use a task-local working map rather than maintaining another
permanent product inventory.

Account for every existing action in that map. Review coherent journeys across
scopes, including these representative cases:

- Switch between a Project's Environments and identify which one an action affects.
- Use a Project Secret in a Service variable while understanding source and exposure.
- Connect a Service to a Backing Service and find that consumer's connection values.
- Mount an Environment Volume from a Service and distinguish saving from deploying.
- Follow a failed deployment into its Task and logs, then return to the workload.
- Find Environment backup and recovery operations without confusing them with shared instance management.

A review must answer: can the operator find the capability without guessing a
catchall category, understand its owner and effect, finish the task, and return
without rebuilding context? Include empty, unavailable, pending and failed states;
check narrow layouts and keyboard access for changed interactions. Verify that
the reorganization has not dropped unrelated capabilities.

Observe wrong turns, unexplained labels and unnecessary context switches in
representative operator walkthroughs. Fewer tabs or clicks alone do not establish
success. Follow [delivery verification](delivery.md#verification-ladder); these
review questions do not require static source tests or a full release gate for
every layout change. Record any exception with its concrete reason and affected
workflow so it can be reviewed rather than silently becoming another convention.

## Rationale and references

Predictable navigation and consistent names reduce the need to relearn controls;
see W3C's guidance on [consistent navigation](https://www.w3.org/WAI/WCAG22/Understanding/consistent-navigation.html)
and [consistent identification](https://www.w3.org/WAI/WCAG22/Understanding/consistent-identification.html).
Visible choices and context support [recognition rather than recall](https://www.nngroup.com/articles/recognition-and-recall/).
[Progressive disclosure](https://www.nngroup.com/articles/progressive-disclosure/)
informs focused detail, with the Groundplane-specific requirement that capability
destinations stay discoverable.

These principles favor a visible task structure with contextual editing over
either one page containing every form or many tiny pages. The tradeoff is that
navigation and summaries need deliberate maintenance as capabilities grow.
The sources inform this standard; they do not prescribe Groundplane's menu tree
or prove that the current Console is usable or accessibility-conformant.
