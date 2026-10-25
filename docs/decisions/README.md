# Technical decisions

These topics explain significant boundaries, their reasons and consequences.
They point to implementation owners instead of reproducing the code. Start with
the relevant [operator guide](../README.md#operators), not every decision file.

| Topic | Question answered |
| --- | --- |
| [Platform runtime](platform-runtime.md) | Who owns Controller, Agent, etcd, identity and observations? |
| [API and Console](api-and-console.md) | Which sources own human interfaces and production UI delivery? |
| [Storage and idempotency](storage-and-idempotency.md) | How do stable identity, persistence and replay prevent duplicate changes? |
| [Component boundaries](component-boundaries.md) | Why do compiled integrations use a closed public SDK? |
| [Networks and shared access](network-and-shared-access.md) | How are pools, backing access and authentication separated? |
| [Release packaging](release-packaging.md) | How are artifacts selected, installed and safely activated? |
| [Task execution](task-execution-redesign.md) | How do scheduling, typed execution, history and a coordinated breaking cutover fit together? Implementation in progress; not qualified. |
| [Desired-state publication](desired-state-publication.md) | How do revisions, publication and accepted latest-wins work differ? |
| [Service releases and recovery](service-release-and-recovery.md) | Which exact runtime can deploy, rollback and recovery select? |
| [Configuration and Scripts](configuration-materialization-and-scripts.md) | Why are file sources, Script starts and cleanup pinned separately? |
| [Resource deletion](resource-deletion.md) | Why does cleanup finish before ownership is released? |
| [Backing provisioning](backing-service-provisioning.md) | How do shared networking, credential ownership and hooks compose? |
| [Backup recovery](backup-recovery.md) | What must make stored recovery data and destructive restore trustworthy? |
| [Runner isolation](runner-isolation.md) | What must isolate CI jobs and retain safe cleanup ownership? |

Accepted design is not a delivery claim. Backup and Runner protocols retain
unimplemented requirements; one compact Backup wire reference remains because the
accepted fields do not yet exist in source. Latest-wins is not connected end to end.
Existing-Zone backing creation remains deferred.
Proposed storage mechanisms do not become approved merely by being described.

Current limits belong in [capabilities](../capabilities.md). Git retains the
superseded numbered records; they are no longer parallel documentation owners.
