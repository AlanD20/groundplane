# Verification feature map

| Capability | Journey | Entry point | Proves |
| --- | --- | --- | --- |
| C01-lite Host health | [host-health.md](host-health.md) | `scripts/host-health-ssh.sh` | Remote Controller reachability, healthy host projection, and API/CLI JSON parity |
| C01/C18/C19 foundation host | [foundation-host.md](foundation-host.md) | `scripts/foundation-host-acceptance.sh` | Read-only host ownership observation and explicitly gated lifecycle/task probes |
| C07 Network Zone and Route | [network-zone-route.md](network-zone-route.md) | `scripts/network-zone-route-etcd.sh` | Real-etcd concurrent replay, subnet isolation, and restart durability |
| C08 Managed Volumes | [volume-lifecycle.md](volume-lifecycle.md) | `scripts/volume-lifecycle-ssh.sh` | API/CLI Volume lifecycle parity, stable identity and path, bounded impact removal, completed Tasks, and host cleanup |
| C11 Attach L2 | [c11-attach-l2.md](c11-attach-l2.md) | `scripts/c11-attach-l2.sh` | Durable Attach identity, ownership, facts, grants, retry/detach ordering, isolation, and surface parity |

Add a journey only after the capability exists in `docs/capabilities.md`.
Prefer extending an existing operator journey over creating another script for
the same boundary.
