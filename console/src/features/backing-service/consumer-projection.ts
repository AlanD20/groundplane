import type { Project, Tenant } from "@/lib/types";
export function backingConsumers(
  backingProjectId: string,
  projects: Project[],
  tenants: Tenant[],
): NonNullable<Project["consumers"]> {
  return projects.flatMap((project) => {
    const tenant = tenants.find(
      (candidate) => candidate.id === project.tenantId,
    );
    return (project.environments ?? []).flatMap((environment) =>
      environment.attaches
        .filter((attach) => attach.backingProjectId === backingProjectId)
        .flatMap((attach) => {
          const connectionFactKey = attach.factSets
            .find((set) => !set.grantAttachId)
            ?.facts.find((fact) => fact.key.endsWith("_URL"))?.key;
          return [
            {
              tenant: tenant?.slug ?? project.tenantId ?? "",
              project: project.slug,
              environment: environment.name,
              service: attach.service,
              attachId: attach.id,
              database: attach.database,
              role: attach.role,
              connectionFactKey,
            },
          ];
        }),
    );
  });
}

export function backingConsumerRevision(
  projects: Project[],
  tenants: Tenant[],
): string {
  return JSON.stringify({
    tenants: tenants.map((tenant) => [tenant.id, tenant.slug]),
    projects: projects.map((project) => [
      project.id,
      project.tenantId ?? "",
      project.slug,
      (project.environments ?? []).map((environment) => [
        environment.id,
        environment.name,
        environment.attaches.map((attach) => [
          attach.id,
          attach.backingProjectId,
          attach.service,
          attach.database,
          attach.role,
          attach.factSets.map((set) => [
            set.grantAttachId ?? "",
            set.facts
              .filter((fact) => fact.key.endsWith("_URL"))
              .map((fact) => fact.key),
          ]),
        ]),
      ]),
    ]),
  });
}
