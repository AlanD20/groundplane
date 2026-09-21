# Console and operator API

The production Console is the React/Vite application in `console/`. It calls
the same Controller API as the CLI. A page or generated type does not replace a
missing Controller capability.

## Using the Console

Navigation follows Platform or Tenant → Project → Environment. Host owns
Controller and Agent management; Components owns integration settings.
Activity and Task views show the same durable operations, with scope and status
filters. Opening a Task shows its details; its resource link opens the affected
page.

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
qualification limits. The recent UI/code restructuring was not build- or
runtime-verified. Basic rendering and wiring do not require dedicated tests;
follow [testing policy](../delivery.md#testing-policy).
