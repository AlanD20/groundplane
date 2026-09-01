# ADR 0004: Console verification stack

Status: accepted

## Context

The Console is a product contract. Typechecking and visual inspection cannot
prove action wiring, accessibility, routing, or responsive behavior.

## Decision

Use Vitest for unit tests, React Testing Library for component and interaction
tests, and Playwright for production-build browser scenarios. Agents use Chrome
DevTools MCP for interactive diagnosis, but committed acceptance tests run
through Playwright.

## Consequences

- Store/client behavior, form validation, and accessibility get fast focused
  tests.
- Every route shape and critical mutation has a browser scenario at desktop
  and mobile widths.
- Browser tests run against the built SPA, not Vite-only development behavior.
