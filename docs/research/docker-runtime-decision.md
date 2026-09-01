# Docker Runtime Decision Research

**Date:** 2026-08-16
**Scope:** Docker Compose Specification, current Docker Compose CLI source, Docker Engine APIs, and Docker Swarm/SwarmKit.
**Recommendation:** Use local `docker compose` as Groundplane's MVP execution primitive on one host, but keep Groundplane's Controller and Agent responsible for desired state, release strategy, health policy, cross-project dependencies, identity, and reconciliation. Do not treat Compose's `deploy` section as a replacement for that logic. Treat Swarm as an optional future runtime, not the MVP default.

## Executive Findings

- Current Docker Compose does support replicas without Swarm. `deploy.replicas` is translated into the number of ordinary containers that Compose creates for a service. It does not create a Docker service, scheduler, or independent reconciliation loop.
- `docker compose up --scale SERVICE=N` overrides the model's scale for that invocation. `docker compose scale SERVICE=N` changes the in-memory project model and reconciles containers; it does not edit the Compose file.
- Local Compose secrets and configs are not equivalent to Swarm secrets and configs. Current Compose mounts file-backed values as read-only host bind mounts and copies environment/content values into a container before start. They are not encrypted Docker objects.
- Compose service `labels` go onto every service container. `deploy.labels` are service-level metadata for a platform with a native service object and do not become local container labels. Current Compose also has a distinct `annotations` field, mapped to Docker Engine container annotations.
- Compose provides useful topology and startup primitives: networks, DNS aliases, healthchecks, `depends_on` conditions, profiles, volumes, labels, and project scoping. These are not a full release controller or runtime reconciler.
- Swarm adds a manager-backed desired-state service model, scheduling, multi-host networking and discovery, restart/replacement, placement, rolling update and rollback machinery, and encrypted secret/config distribution. It still does not provide Groundplane's domain-level release ledger, blue-green route switch, cross-project DAG, or product-specific policies.

## 1. Replicas And Scaling

### Specification behavior

The Compose Deploy Specification calls `deploy` optional: an implementation that does not implement it may ignore it while still accepting the file. Its `replicas` definition says that, for a replicated service, the number is the number of containers that should be running at a given time.

- [Compose Deploy Specification: optional deploy, mode, replicas](https://compose-spec.github.io/compose-spec/deploy.html)
- [Compose services reference: deploy is optional and may be ignored](https://docs.docker.com/reference/compose-file/services/#deploy)

That wording is platform-neutral. It does not imply Swarm, nor does it require every implementation to provide a controller.

### Current non-Swarm Compose behavior

Current Docker Compose implements the field locally. The Compose model's `GetScale()` returns, in order, the explicit `scale` value, `deploy.replicas`, or `1`. The CLI applies `--scale SERVICE=N` by calling `SetScale`, and the convergence code comments that it reconciles containers by "re-creating container, adding or removing replicas, or starting stopped containers."

- [compose-go `ServiceConfig.GetScale` and `SetScale`](https://github.com/compose-spec/compose-go/blob/main/types/types.go)
- [Docker Compose `cmd/compose/create.go`: `--scale` and `applyScaleOpts`](https://github.com/docker/compose/blob/main/cmd/compose/create.go)
- [Docker Compose `pkg/compose/convergence.go`: local scale and convergence](https://github.com/docker/compose/blob/main/pkg/compose/convergence.go)
- [Docker Compose `docker compose up` reference: `--scale`](https://docs.docker.com/reference/cli/docker/compose/up/)

Therefore, with no Swarm enabled:

```yaml
services:
  api:
    image: example/api
    deploy:
      replicas: 3
```

`docker compose up -d` creates three ordinary containers, conventionally named like `<project>-api-1`, `<project>-api-2`, and `<project>-api-3`. The Docker Engine sees containers, not a service with desired replica state.

`docker compose up --scale api=5` overrides the Compose model's scale for that run. `docker compose scale api=5` parses the argument, calls `SetScale(5)`, and invokes the Compose backend's scale operation. Neither command changes the YAML on disk.

- [Docker Compose `cmd/compose/scale.go`](https://github.com/docker/compose/blob/main/cmd/compose/scale.go)

Important local limitations:

- A service with `container_name` cannot be scaled beyond one container; Compose returns an error.
- Fixed host port bindings can make multiple replicas impossible because ordinary containers each need a host binding. Dynamic host ports can be used, but then Groundplane must discover the assigned ports.
- Compose's reconciliation occurs when an operation such as `up` or `scale` is run. Compose does not continuously recreate a container removed out-of-band or replace a container merely because it is unhealthy. Engine restart policy can restart an exiting process, but that is not the same as a project controller.
- `docker compose up --wait` waits for services to be running or healthy and implies detached mode. It is a command completion gate, not a continuing health controller.

- [Compose `container_name` scaling restriction](https://docs.docker.com/reference/compose-file/services/#container_name)
- [Compose networking reference: dynamic ports per scaled replica](https://docs.docker.com/compose/how-tos/networking/#debugging)
- [Compose `up --wait`](https://docs.docker.com/reference/cli/docker/compose/up/)

### Version-sensitive point

Older Compose implementations commonly ignored `deploy` outside Swarm. That is no longer a safe assumption for current Docker Compose. Groundplane must pin and test a minimum Compose plugin version if it emits `deploy.replicas`, and should use `docker compose config` plus an execution smoke test as part of capability validation.

## 2. Secrets And Configs In Local Compose

### Compose model

The specification requires explicit service grants. A top-level declaration alone does not expose a secret or config to a container.

Secrets support these top-level sources:

- `file`: content is read from a host file.
- `environment`: content is read from a host environment variable.
- `external: true`: refer to a pre-existing platform secret.
- `name`: override the platform lookup name.

Configs support:

- `file`: content is read from a host file.
- `environment`: content is read from a host environment variable.
- `content`: inline content.
- `external: true`: refer to a pre-existing platform config.
- `name`: override the platform lookup name.

The `environment` and `content` config sources are explicitly versioned as Docker Compose 2.23.1 features. The secret `environment` source is marked Compose 2.6.0 in the specification.

- [Compose Specification: secrets](https://compose-spec.github.io/compose-spec/09-secrets.html)
- [Compose Specification: configs](https://compose-spec.github.io/compose-spec/08-configs.html)
- [Docker reference: secrets top-level element](https://docs.docker.com/reference/compose-file/secrets/)
- [Docker reference: configs top-level element](https://docs.docker.com/reference/compose-file/configs/)

### Mount semantics

For a service grant, the standard default paths are:

- Secret: `/run/secrets/<name>` on Linux.
- Config: `/<name>` on Linux.
- Windows defaults use `C:\...` paths as specified by the service reference.
- Long syntax can select `target`, `uid`, `gid`, and `mode`.

The Compose specification describes configs as file mounts, normally read-only and mode `0444`. Docker's Compose secrets how-to says secrets are delivered as a single-file bind mount and therefore are supported only for Linux containers. A service can only see entries explicitly listed under its `secrets` or `configs` key.

- [Compose service `secrets` and `configs` syntax](https://docs.docker.com/reference/compose-file/services/#configs)
- [Compose secrets how-to and Linux-only limitation](https://docs.docker.com/compose/how-tos/use-secrets/)

### What current Docker Compose actually does

The current Docker Compose source is more specific than the platform-neutral specification:

- A file-backed local secret/config is represented as a read-only bind mount from the host path. The source path is resolved on the Compose client and is not an engine-managed secret/config object.
- An environment-backed secret, or an environment/content-backed config, is copied into the created container before the container starts using `CopyToContainer` and a tar entry. The default tar mode is `0444`.
- Current local Compose rejects `external` service resources in this path with `unsupported external secret ...` or `unsupported external config ...` rather than looking up a Swarm object.
- Current local Compose rejects `driver` and `template_driver` for these local file references.
- For file-backed values, `uid`, `gid`, and `mode` cannot be remapped through the bind mount and current code warns that they are ignored. This is the limitation called out by the Compose service reference.
- Content copied into the container is lost when that container is removed. A host file bind mount survives container removal because the source file belongs to the host. `docker compose down` removes the project containers and managed networks, not the source files.

- [Docker Compose `pkg/compose/create.go`: local bind mounts, unsupported external/driver fields, and `uid`/`gid`/`mode` warnings](https://github.com/docker/compose/blob/main/pkg/compose/create.go)
- [Docker Compose `pkg/compose/secrets.go`: `CopyToContainer`, tar mode, and pre-start injection](https://github.com/docker/compose/blob/main/pkg/compose/secrets.go)
- [Compose volumes and `down` lifecycle](https://docs.docker.com/reference/cli/docker/compose/down/)

This has two practical consequences for Groundplane:

1. Local Compose's `secrets` syntax is a useful injection contract, but it is not a secret manager. A file source remains a host file and an environment/content source passes through the local Compose process and container creation path.
2. Groundplane's own secret store, reveal rules, materialization permissions, rotation, and cleanup remain necessary. If Groundplane needs an encrypted, non-host-file runtime secret, local Compose alone does not provide it.

### Documentation/version discrepancy

The current Compose Specification and Docker reference document `configs` for Compose, and current Compose source implements local file/environment/content configs. The Docker Swarm configs page still says that the `configs` key is not supported by `docker compose`, while describing it as supported by `docker stack`. That statement conflicts with the current Compose source and current Compose reference. Treat it as a stale or scope-specific statement, and pin the tested Compose plugin version rather than relying on prose alone.

- [Docker Swarm configs page, conflicting statement](https://docs.docker.com/engine/swarm/configs/)
- [Current Compose config implementation](https://github.com/docker/compose/blob/main/pkg/compose/create.go)

## 3. Labels And Annotations

### Labels

Compose service-level `labels` are container metadata. Current Docker Compose passes the user labels into `container.Config.Labels`, so each replica receives them. Compose also adds canonical labels, including:

- `com.docker.compose.project` on all resources created by Compose.
- `com.docker.compose.service` on service containers.
- Internal labels such as config hash and container number, used to identify and reconcile replicas.

The `com.docker.compose` prefix is reserved and user-supplied labels with that prefix are rejected by the Compose specification.

- [Compose service labels](https://docs.docker.com/reference/compose-file/services/#labels)
- [Docker Compose source: container labels and canonical resource labels](https://github.com/docker/compose/blob/main/pkg/compose/create.go)
- [Docker Compose source: project/service label filters](https://github.com/docker/compose/blob/main/pkg/compose/labels.go)

Resource labels are not one global label namespace:

- Top-level network `labels` are stored on the Docker network. Compose adds project and network identity labels.
- Top-level named volume `labels` are stored on the Docker volume. They do not apply to bind mounts.
- Local Compose creates containers, networks, and named volumes as the managed Docker resources. It does not create Docker secret/config objects for the local file/environment/content paths described above.
- `deploy.labels` are different. The Deploy Specification says they are only set on the platform service, not on containers, assuming the platform has a native service concept. Local `docker compose up` has no Docker service object, so these labels do not provide a local container label channel.

- [Compose network labels](https://docs.docker.com/reference/compose-file/networks/#labels)
- [Compose volume labels and bind-mount limitation](https://docs.docker.com/reference/compose-file/volumes/#labels)
- [Compose Deploy Specification: deploy labels are service-only](https://compose-spec.github.io/compose-spec/deploy.html#labels)

### Annotations

There is a distinct annotation concept in current Compose. `services.<name>.annotations` defines annotations for the container. Current Compose maps that field to Docker Engine `HostConfig.Annotations`. Moby defines those as "arbitrary non-identifying metadata attached to container and provided to the runtime." That is intentionally different from labels, which are identifying metadata visible to Docker object inspection and used by Compose's reconciliation filters.

- [Compose service annotations](https://docs.docker.com/reference/compose-file/services/#annotations)
- [Docker Compose source: `service.Annotations` mapped to `HostConfig.Annotations`](https://github.com/docker/compose/blob/main/pkg/compose/create.go)
- [Moby Engine `HostConfig.Annotations`](https://github.com/moby/moby/blob/master/api/types/container/hostconfig.go)

Swarm also has an `Annotations` structure on service specs and separate container labels in `ContainerSpec.Labels`; this is another reason not to collapse labels and annotations in Groundplane's resource model.

- [SwarmKit `ServiceSpec`, `ContainerSpec.labels`, and annotation model](https://github.com/moby/swarmkit/blob/master/api/specs.proto)

## 4. Compose Feature Surface Relevant To Groundplane

| Feature | Current Compose behavior | Groundplane implication |
| --- | --- | --- |
| Networks | Creates an implicit project-scoped `default` bridge network unless networking is otherwise specified. Top-level networks can select drivers, IPAM, `internal`, `attachable`, custom names, and `external` pre-existing networks. | Compose owns same-project network creation. Groundplane still owns cross-project network identity and lifecycle decisions. |
| Aliases | A service is discoverable by service name on a shared network. `networks.<network>.aliases` adds network-scoped hostnames. An alias can be shared by multiple containers/services, in which case resolution is not guaranteed to select one particular container. | Use stable service/alias names, not replica IPs. Blue-green switching needs a route/alias policy outside ordinary Compose service DNS. |
| Healthchecks | Container health is evaluated using the image `HEALTHCHECK` or service override. `docker compose up --wait` waits for running/healthy. | Health is observable and can gate an operation, but Compose does not turn it into Groundplane's release policy or durable health ledger. |
| Dependencies | `depends_on` controls create/start and removal order. Long syntax supports `service_started`, `service_healthy`, and `service_completed_successfully`; `restart: true` applies to explicit Compose dependency updates. | Same-project startup gates can compile to native Compose. Cross-project dependencies remain Controller/Agent logic. |
| Profiles | Services without profiles are enabled by default. Profiled services start only when a profile is enabled, except an explicitly targeted service and its dependencies. | Preserve profiles; do not silently activate or drop them during rendering. |
| Configs | Explicit service grants. Current local implementation uses read-only bind mounts for files and pre-start copy for environment/content. External, driver, and template-driver behavior is implementation/version sensitive locally. | Keep Groundplane materialization and capability checks. |
| Secrets | Explicit service grants. Current local implementation is a host bind mount or pre-start copy, not encrypted engine secret storage. | Groundplane secret store remains authoritative for MVP. |
| Volumes | Named volumes are created/reused and persist independently of a container. `external` volumes are looked up and not created/owned by Compose. `down -v` is a destructive opt-in for project volumes. | Stable volume identity and safe deletion policy need Groundplane ownership metadata and confirmation. |
| Labels | Service labels propagate to every service container; network/volume labels stay on those resources. Compose canonical labels are used for project/service matching. | Use labels as execution identity hints, not as the complete durable identity model. |
| Project names | Precedence is `-p`, `COMPOSE_PROJECT_NAME`, top-level `name`, Compose-file directory, then current directory. The name is exposed as `COMPOSE_PROJECT_NAME`; default resource names are project-scoped unless an explicit `name` is used. | Groundplane should set an explicit project name derived from stable IDs, not a filesystem directory or mutable slug. |

Primary references for the table:

- [Compose networking how-to](https://docs.docker.com/compose/how-tos/networking/)
- [Compose service attributes: aliases, healthcheck, depends_on, networks, labels, profiles](https://docs.docker.com/reference/compose-file/services/)
- [Compose profiles how-to](https://docs.docker.com/compose/how-tos/profiles/)
- [Compose volumes reference](https://docs.docker.com/reference/compose-file/volumes/)
- [Compose project-name precedence](https://docs.docker.com/compose/how-tos/project-name/)
- [Compose version and name reference](https://docs.docker.com/reference/compose-file/version-and-name/)

The Compose service reference explicitly says short `depends_on` waits for a dependency to be started, not healthy; `service_healthy` is the health-gated form. This distinction matters when compiling Groundplane's dependency phases.

## 5. What Swarm Adds

### Runtime capabilities

Swarm mode is a cluster manager built into Docker Engine. A manager stores desired service state, schedules tasks, and reconciles actual state. The official overview gives the concrete example that if two of ten replica hosts fail, the manager creates two replacement replicas. A single-node swarm is supported, but the node still has to be initialized as a manager.

- [Docker Swarm mode overview: declarative model, scaling, reconciliation, rolling updates](https://docs.docker.com/engine/swarm/)
- [Swarm key concepts: managers, tasks, desired state, service discovery](https://docs.docker.com/engine/swarm/key-concepts/)
- [Swarm node behavior and single-manager operation](https://docs.docker.com/engine/swarm/how-swarm-mode-works/)
- [SwarmKit service spec: replicated desired state and task healthcheck semantics](https://github.com/moby/swarmkit/blob/master/api/specs.proto)
- [SwarmKit task orchestration: failed/deleted tasks are reconciled](https://github.com/moby/swarmkit/blob/master/manager/orchestrator/replicated/tasks.go)

Swarm adds:

- Replicated and global services with a manager-controlled desired task count.
- Placement constraints, placement preferences, resource reservations, and maximum replicas per node.
- Overlay networking, service DNS, VIP or DNS round-robin endpoint modes, and routing-mesh published ports.
- Restart/replacement after task failure and rescheduling when a node becomes unavailable.
- Rolling updates with parallelism, delay, monitor period, failure action, and `start-first` or `stop-first` order.
- Manual and automatic rollback using a prior service specification.
- Docker-managed secrets encrypted in transit and at rest in the Swarm Raft log, mounted only for authorized service tasks, and removed from the task's in-memory filesystem when the task stops.
- Docker-managed configs distributed through the swarm, mounted as files, immutable, and not encrypted at rest. Configs and secrets are limited to 500 KB by the SwarmKit object definitions.

- [Swarm services: placement, replicas, updates, rollback, and health monitoring](https://docs.docker.com/engine/swarm/services/)
- [Swarm `docker service create`: replicas, placement, healthcheck, secrets, configs](https://docs.docker.com/reference/cli/docker/service/create/)
- [Swarm `docker service update`: rolling update and rollback controls](https://docs.docker.com/reference/cli/docker/service/update/)
- [Swarm secrets lifecycle and encryption](https://docs.docker.com/engine/swarm/secrets/)
- [Swarm configs lifecycle and immutability](https://docs.docker.com/engine/swarm/configs/)
- [SwarmKit update supervisor source](https://github.com/moby/swarmkit/blob/master/manager/orchestrator/update/updater.go)
- [SwarmKit restart supervisor source](https://github.com/moby/swarmkit/blob/master/manager/orchestrator/restart/restart.go)

### What Swarm does not remove for Groundplane

**One host:** Swarm can run on one host, but it adds manager state, swarm initialization, service objects, and an extra operational mode without adding placement or fault-domain value. Local Compose is simpler for the MVP's one-host contract.

**Blue-green deploys:** Swarm's `start-first` update order can briefly overlap old and new tasks, but it is a rolling update within one service. It does not create two independently named releases and atomically switch a Groundplane route or alias between them. Groundplane still needs blue-green slot identity, traffic switching, rollback policy, and cleanup.

**Rolling updates:** Swarm can own task-level rolling replacement. Groundplane still needs to decide when a logical release is being deployed, which image/version is desired, how the deployment is authorized and recorded, and how it interacts with project-level dependencies and routes.

**Health gates:** Swarm uses container healthchecks to fail unhealthy tasks and its update monitor to observe task failure. Its native model is not Groundplane's arbitrary cross-project health-gate DAG. Also, `depends_on` is a Compose startup construct and is not a substitute for a Controller-level dependency graph when projects are separate.

**Secrets/configs:** Swarm supplies stronger engine-managed objects, but Groundplane still needs domain ownership, secret references, rotation workflow, audit/reveal rules, and policy for what is allowed into a service. Swarm configs are not encrypted at rest, and its secrets/configs are immutable objects that require replacement for rotation.

**Placement:** Swarm placement is valuable for multiple nodes. It does not replace Groundplane's host selection, Agent pairing, project policy, or resource ownership model. On one host, it is mostly redundant.

**Reconciliation:** Swarm reconciles Swarm service tasks. It does not reconcile Groundplane's Controller records, generated Compose projects, external networks, router state, backup policy, or cross-project operations. Groundplane remains a control plane even if it later uses Swarm as one adapter.

### Stack deploy is a separate path

`docker stack deploy` is explicitly a Swarm manager command and translates a Compose file into Swarm services. The CLI documents unsupported Compose options being ignored, and current Compose documents `extends` as unsupported for stack deploy. A Groundplane adapter cannot assume that a Compose file accepted by local `docker compose up` has identical semantics under `docker stack deploy`.

- [Docker `stack deploy`: Swarm-only command and unsupported-option behavior](https://docs.docker.com/reference/cli/docker/stack/deploy/)
- [Compose `extends`: unsupported by `docker stack deploy`](https://docs.docker.com/reference/compose-file/services/#extends)

## 6. Maintenance Risks

### Compose risks

- **Spec versus implementation drift.** The specification is deliberately platform-neutral and says optional deploy fields may be ignored. Current Compose implements more fields than older versions did, so behavior changes with the plugin version.
- **Documentation drift.** The current Swarm configs page conflicts with current Compose source/reference about local configs. This is an operational signal to test behavior, not just parse documentation.
- **Plugin/Engine/API skew.** New Compose fields and Engine features have explicit Compose or Engine version badges. An older plugin may warn, ignore, or reject a field; an older Engine may reject the generated API request.
- **No long-running local controller.** Compose's convergence is operation-driven. It is not a daemon that continuously repairs the project after out-of-band deletion, host restart edge cases, or unhealthy-but-running application state.
- **Port and storage semantics under scaling.** Ordinary containers share one host. Fixed published ports collide, and local named volumes are shared by replicas unless the service intentionally uses separate volume names or another storage design.
- **Local secret exposure.** File-backed secrets are host files; content/environment values are handled by the local client and copied into a container. This is materially weaker than Swarm's encrypted object and in-memory mount model.

### Swarm risks

- **Operational complexity for the MVP.** A one-host manager adds bootstrap, cluster state, service/stack lifecycle, and Swarm-specific troubleshooting.
- **Semantic fork.** Local Compose and stack deploy accept overlapping but non-identical Compose subsets. Supporting both as first-class runtimes multiplies conformance tests and capability decisions.
- **Storage remains host/plugin dependent.** Swarm scheduling can move a task, but a local volume or bind mount may not exist or contain the same data on another node. Swarm does not make stateful storage portable.
- **Rolling does not mean blue-green.** Swarm's update controller is strong at task replacement but does not provide Groundplane's release and traffic model.
- **Version-sensitive Swarm behavior.** CLI flags are tied to Engine API versions, and newer fields such as health start interval, memory controls, or job modes have explicit API version markers.

## Recommendation For Groundplane

1. **MVP runtime:** use `docker compose up` on one host through the Agent. Generate an explicit stable project name, use Compose labels as execution hints, and keep durable IDs in Groundplane's Controller records and `x-gp-*` metadata.
2. **Groundplane-owned logic:** retain desired-state persistence, operation serialization/idempotency, cross-project dependency ordering, blue-green slots and route switching, health-gate policy, rollback decisions, secret store/materialization, and reconciliation after out-of-band changes.
3. **Compose boundary:** emit a tested native Compose document and preserve valid fields broadly, with explicit MVP policy exceptions. Validate with `docker compose config`; then apply and explicitly wait for the health conditions Groundplane requires. Do not rely on `deploy.update_config`, `deploy.rollback_config`, placement, or `deploy.labels` in local Compose as if they were Swarm features.
4. **Secrets/configs:** preserve native syntax where it is useful, but treat local Compose `secrets` and `configs` as a mount/copy transport only. Groundplane should materialize files with its own permissions and cleanup rules, and should not claim Swarm-grade secrecy from local Compose.
5. **Future Swarm adapter:** consider Swarm only if Groundplane's supported deployment target expands to multiple Docker nodes and the team accepts a second runtime semantic surface. If added, implement it as an adapter with explicit capability reporting, not as a reason to delete Controller logic.

## Source Set

Primary sources used in this note:

- [Compose Specification](https://compose-spec.github.io/compose-spec/)
- [Compose Deploy Specification](https://compose-spec.github.io/compose-spec/deploy.html)
- [Compose services](https://compose-spec.github.io/compose-spec/05-services.html)
- [Compose networks, volumes, configs, and secrets](https://compose-spec.github.io/compose-spec/06-networks.html), [volumes](https://compose-spec.github.io/compose-spec/07-volumes.html), [configs](https://compose-spec.github.io/compose-spec/08-configs.html), [secrets](https://compose-spec.github.io/compose-spec/09-secrets.html)
- [Docker Compose CLI reference](https://docs.docker.com/reference/cli/docker/compose/), [`up`](https://docs.docker.com/reference/cli/docker/compose/up/), [`scale`](https://docs.docker.com/reference/cli/docker/compose/scale/), [`config`](https://docs.docker.com/reference/cli/docker/compose/config/)
- [Docker Compose how-tos](https://docs.docker.com/compose/how-tos/networking/), [secrets](https://docs.docker.com/compose/how-tos/use-secrets/), [profiles](https://docs.docker.com/compose/how-tos/profiles/), [project names](https://docs.docker.com/compose/how-tos/project-name/)
- [Docker Compose source](https://github.com/docker/compose), especially [`cmd/compose/create.go`](https://github.com/docker/compose/blob/main/cmd/compose/create.go), [`cmd/compose/scale.go`](https://github.com/docker/compose/blob/main/cmd/compose/scale.go), [`pkg/compose/create.go`](https://github.com/docker/compose/blob/main/pkg/compose/create.go), [`pkg/compose/convergence.go`](https://github.com/docker/compose/blob/main/pkg/compose/convergence.go), [`pkg/compose/secrets.go`](https://github.com/docker/compose/blob/main/pkg/compose/secrets.go), and [`pkg/compose/containers.go`](https://github.com/docker/compose/blob/main/pkg/compose/containers.go)
- [Moby Engine source](https://github.com/moby/moby), especially [`HostConfig`](https://github.com/moby/moby/blob/master/api/types/container/hostconfig.go)
- [Docker Swarm documentation](https://docs.docker.com/engine/swarm/), [services](https://docs.docker.com/engine/swarm/services/), [secrets](https://docs.docker.com/engine/swarm/secrets/), [configs](https://docs.docker.com/engine/swarm/configs/), [`stack deploy`](https://docs.docker.com/reference/cli/docker/stack/deploy/), and [`service update`](https://docs.docker.com/reference/cli/docker/service/update/)
- [Moby SwarmKit source](https://github.com/moby/swarmkit), especially [`api/specs.proto`](https://github.com/moby/swarmkit/blob/master/api/specs.proto), task orchestration, [restart supervisor](https://github.com/moby/swarmkit/blob/master/manager/orchestrator/restart/restart.go), and [update supervisor](https://github.com/moby/swarmkit/blob/master/manager/orchestrator/update/updater.go)
