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
An Environment opens on Services, with Cards and Table display options, runtime
state and direct access to each workload. Backing-connection links on either
display open the Service’s connections within the Environment. Activity and backup
management remain in Tasks and Backups & restore. A compact vertical icon rail
provides Platform navigation beside a labeled secondary menu for the selected resource’s
destinations. Global pages without local sections, such as the Backing Services
list, show only the Platform rail. Environment destinations are visible links
under headings, including Variables & files, backing connections
and their Connection values, Deployments, Backup destinations, and Identity &
encryption key. Headings organize links without adding intermediate pages.
Breadcrumbs switch Tenant, Project, Environment and Service scope. On narrow
screens, Navigate opens the same scrollable navigation.

Opening a Service replaces Environment navigation with that Service’s destinations:
Overview, Logs, Deployments, Variables & files, Storage, Backing connections,
Runtime & image, Networking and Configuration report. Runtime, healthcheck,
networking and custom lifecycle hooks use focused editors. Start/Stop and Deploy
remain visible actions. Variables & files lists Entries exposed to the Service,
including shared Entries; editing a shared Entry affects its full exposure.
The source editor offers Direct value, Reusable Secret and Connection value.
Connection selectors use published Attach metadata; Secret references offer
suggestions without revealing plaintext.
Storage selects Environment Volumes, container paths and read-only access. Removing
a mount retains the Volume and its data. File Entries remain in Variables & files.
Backing connections lists this Service’s Attaches, opens the shared connect editor
with the Service selected, and shows its connection values. Reveal, Hide and Copy
use the shared value control and configured reveal confirmation.

Project Secrets distinguish Project-owned inputs from matching-key Platform
fallbacks. Runners appears in Tenant, Project and Environment navigation. Project
and Environment views show Runners owned by that resource and preselect that
fixed owner when creating one. The Tenant view retains its cross-scope owner
filter. These views reuse the same lifecycle actions without leaving the selected
workspace. Use parent links or breadcrumbs to reach other scopes. Tasks
show durable
operations with scope and status filters. Opening a Task shows its details and
links to the affected resource. A deployment opened from a Service returns to
that Service’s history. Saving desired configuration does not deploy it.

Backups shows protection, schedule, sources and Recovery Points first. Edit policy
opens the policy drawer. Backup destinations directly opens the Environment’s
Connector manager; the same manager is also available beside backup details.
Named disclosures retain encryption information and adapter steps.

Backing Services open with internal connection information, connected
applications, containers and persistent storage. Their sidebar separates Logs,
Backing connections, consumer Backups & restore, Runtime & image, Networking and
Configuration report. Consumer backup rows link to the owning Environment’s
recovery workflow. Runtime editors identify the shared impact, and Destroy runtime
retains its explicit confirmation and data-retention explanation. Destinations
are retained in the URL, including CoreDNS’s Overview, DNS records and Resolver
settings views. Console preferences contains browser appearance and reveal
confirmation; resource management remains in its named destination.

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
