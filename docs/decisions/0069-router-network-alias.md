# ADR 0069: Optional router network alias

- Status: Accepted
- Date: 2026-09-08
- Capability: Environment HTTP router

An operator may configure one optional DNS-label `alias` for the HTTP router.
The alias belongs to portable router settings, exists only on the primary
selected Zone, and is retained through disable/re-enable and reconstruction.
It does not rename the generated Service, change its stable ID, or change ports.
Empty removes it; omitted CLI flags preserve the loaded declaration, while a
complete Blueprint omitting it declares no custom alias.

The existing Configure and Enable actions carry it through Console, CLI and
API. Blueprint uses `x-gp-components.http-router.settings.alias`. API clients
are regenerated from the typed contract. Registered Caddy consumes the value
through its public configuration and emits an existing typed network-alias
attachment; no execution authority or runtime plugin mechanism is introduced.

Reject invalid labels and same-Zone collisions with native or generated Service
names/aliases before publication. No host DNS or Cloudflare configuration is
changed. The motivating origin is `http://kobwnewe-router:80`.

Verification is recorded in the operational checkpoint. A deployment build or
live alias check is not a full CI or floor-MVP acceptance claim.
