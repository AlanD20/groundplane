# Verification feature map

| Capability | Journey | Entry point | Proves |
| --- | --- | --- | --- |
| C01-lite Host health | [host-health.md](host-health.md) | `scripts/host-health-ssh.sh` | Remote Controller reachability, healthy host projection, and API/CLI JSON parity |
| C08 Managed Volumes | [volume-lifecycle.md](volume-lifecycle.md) | `scripts/volume-lifecycle-ssh.sh` | API/CLI Volume lifecycle parity, stable identity and path, bounded impact removal, completed Tasks, and host cleanup |

Add a journey only after the capability exists in `docs/capabilities.md`.
Prefer extending an existing operator journey over creating another script for
the same boundary.
