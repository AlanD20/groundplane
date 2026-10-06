# Console and operator API

The production Console is the React/Vite application in `console/`. It calls
the same Controller API as the CLI. A page or generated type does not replace a
missing Controller capability.

## Using the Console

Growing Console tables use five rows per page by default, with 5/10/25/50
choices and visible sort direction on sortable columns. Filters and sorting
apply before pagination. Recovery-point sorting covers loaded records only;
the page indicates when older records remain to be loaded. Small fixed summaries,
including the MVP's single Agent, do not need pagination.

Navigation follows Platform or Tenant → Project → Environment. Host owns
Controller and Agent management; Components owns integration settings.
An Environment opens on a Service overview with Cards and Table display options,
recent activity and backup protection. The left rail selects the platform area; the explorer keeps the
current Project, Environment and Services visible. Horizontal Environment tabs group Network
(Zones, Routes, backing connections and Router), Configuration (Blueprint,
Entries, Attach facts, Volumes and Scripts), Operations (Tasks, Logs, Releases,
Release groups and Backups) and Settings. Project and shared-infrastructure
links remain available in the sidebar. The breadcrumbs switch resource scope.
Activity and Task views show the same durable operations, with scope and status
filters. Opening a Task shows its details; its resource link opens the affected
page.
Opening a Service shows its workspace with Overview, Logs, Deployments,
Variables & files and Settings tabs. Settings shows current values with focused
editors for image, runtime and resources, healthcheck and networking.
Variables & files lists Entries exposed to the Service, including shared Entries;
editing a shared Entry affects its full exposure. Service Settings includes a
Storage editor: select an Environment Volume, set its container path and read-only
access, or remove a mount. File Entries remain in Variables & files. The full
configuration and Compose preview are expandable. Saving configuration does not
deploy it. Long forms retain their action footer while scrolling.

Backups shows protection, schedule, sources and Recovery Points first. Edit policy
opens the policy drawer; expandable destination and encryption sections contain
Connector management, encryption details and adapter steps.

Backing Services open with internal connection information, connected
applications, containers and persistent storage. Connections holds per-consumer
connection details; Logs, Backups and Settings are separate tabs. Settings uses
the same focused editors as Services and contains configuration previews and
the Destroy runtime confirmation. Tab selection is retained in the URL.

Loading, errors, missing data and expired observations are explicit. The Console
must not substitute sample data for unavailable health or claim an operation
succeeded merely because its request was accepted.

Forms group identity, runtime, network, credentials and health fields when those
are separate concerns. Labels and relevant help stay beside their controls.
Zone creation shows the Environment pool bounds and existing Zone subnets;
these bounds do not claim every address is allocatable.

The network view shows each Service in all its memberships and always includes
No zone. Selecting an item highlights related memberships; selecting again
clears the highlight. Lists provide filtering, sortable columns and a visible
sort direction.

## Editing documents

The shared editor supports YAML, JSON and shell highlighting plus read-only
preview. YAML/JSON formatting is an explicit local action: it does not save or
apply the document. Controller validation remains authoritative.
Unsupported languages remain plain text.

The UI uses one plum/lavender palette in light and dark themes, shared controls,
thin focus outlines and reduced-motion-aware transitions. Clicking outside
dialogs and drawers dismisses them. Layout and actions remain usable on mobile.

## API and CLI

[CLI and API usage](../api-cli.md) explains scope, protected requests, pagination,
Task progress and the explicit reveal/bootstrap exceptions. Read the
[feature guides](../README.md#features) for action consequences.

Production embeds the SPA in the Controller binary; it needs neither Node nor
an adjacent asset directory. API paths never fall through to HTML.

## Design and qualification

[Console architecture](../architecture.md#console) owns component and feature
boundaries. [Capability status](../capabilities.md) owns remaining parity and
qualification limits. Basic rendering and wiring do not require dedicated tests;
follow [testing policy](../delivery.md#testing-policy).
