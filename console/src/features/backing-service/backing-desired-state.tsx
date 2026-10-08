import { CopyButton } from "@/components/common/copy-button";
import { ResourcePanel } from "@/components/common/resource-panel";
import { CodeEditor } from "@/components/ui/code-editor";
import { useStore } from "@/lib/store";
import type { Project } from "@/lib/types";
import { valkeyAuthenticationDetails } from "@/lib/valkey-authentication";
import { toYAML } from "@/lib/yaml";

// ---- Desired state ----

export function DesiredStateTab({
  g,
  env,
  svc,
}: {
  g: Project;
  env: NonNullable<Project["environments"]>[number];
  svc: NonNullable<Project["environments"]>[number]["services"][number];
}) {
  const store = useStore();
  const adapter = store.adapters.find((a) => a.key === svc.adapter);
  const authenticationDetails = valkeyAuthenticationDetails(svc.authentication);
  const authenticationUnavailable =
    svc.adapter === "valkey" && !authenticationDetails;
  const doc: Record<string, unknown> = {
    kind: "backing",
    schema: 1,
    metadata: {
      name: svc.serviceName,
      label: g.name,
    },
    backing: {
      name: svc.serviceName,
      adapter: svc.adapter,
      adapter_version: svc.adapterVersion,
      ...(svc.adapter === "valkey"
        ? { authentication: svc.authentication }
        : {}),
      image: svc.image,
      prefix: svc.prefix ?? adapter?.prefix,
      host: svc.serviceName,
      port:
        adapter?.urlScheme === "redis"
          ? 6379
          : adapter?.urlScheme === "pgsql"
            ? 5432
            : adapter?.urlScheme === "mysql"
              ? 3306
              : undefined,
      zones: env.zones.map((z) => ({
        name: z.name,
        subnet: z.subnet,
        internal: z.internal,
      })),
      custom: adapter?.custom ?? false,
      hooks: svc.hooks,
      provision:
        authenticationDetails?.provision ??
        (authenticationUnavailable ? [] : (adapter?.provision ?? [])),
      exposes: adapter?.custom
        ? (svc.hooks?.facts ?? []).map((fact) => fact.key)
        : authenticationDetails
          ? authenticationDetails.factSuffixes.map(
              (suffix) => `${svc.prefix ?? adapter?.prefix}_${suffix}`,
            )
          : authenticationUnavailable
            ? []
            : (adapter?.envVars ?? []),
      healthcheck: svc.healthcheck
        ? { kind: svc.healthcheck.kind, target: svc.healthcheck.target }
        : undefined,
      volume: adapter?.custom ? undefined : "volumes/data",
    },
    consumers: (g.consumers ?? []).map((c) => ({
      project: c.project,
      environment: c.environment,
      service: c.service,
      database: c.database,
      role: c.role,
    })),
  };
  return (
    <ResourcePanel
      title="Desired state"
      actions={<CopyButton value={toYAML(doc)} label="Copy desired state" />}
    >
      <CodeEditor
        id="backing-desired-state"
        label="Backing desired state"
        value={toYAML(doc)}
        readOnly
        language="yaml"
      />
    </ResourcePanel>
  );
}
