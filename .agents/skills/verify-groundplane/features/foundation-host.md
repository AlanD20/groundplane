# C01/C18/C19 foundation host

This journey observes an already provisioned reference host and, only when
explicitly requested, runs the lifecycle and Task probes recorded in the
acceptance document.

Required environment variables are `GP_ACCEPTANCE_HOST`,
`GP_ACCEPTANCE_SSH_KEY`, `GP_ACCEPTANCE_KNOWN_HOSTS`, and
`GP_ACCEPTANCE_OWNERSHIP_MARKER`. Run `trust` first with a new, non-existent
`GP_ACCEPTANCE_KNOWN_HOSTS` path. `observe` is read-only. Every other action
also requires `GP_ACCEPTANCE_MUTATE=1` and the remote ownership marker.

The script never reads or prints Agent token contents. Use a unique local path
for every trust file; an existing trust file is never overwritten.
