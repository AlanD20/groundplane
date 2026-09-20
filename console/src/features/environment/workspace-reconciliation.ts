import type { Project } from "@/lib/types";

export function environmentGenerationSnapshot(
  projects: Project[],
  generations: Map<string, number>,
) {
  return new Map<string, number>(
    projects.flatMap((project) =>
      (project.environments ?? []).map(
        (environment) =>
          [environment.id, generations.get(environment.id) ?? 0] as [
            string,
            number,
          ],
      ),
    ),
  );
}

export function mergeEnvironmentProjectLoads(
  currentProjects: Project[],
  loadedProjects: Project[],
  snapshotGenerations: Map<string, number>,
  shouldPreserve: (
    environmentId: string,
    snapshotGeneration: number,
  ) => boolean,
) {
  const currentByProject = new Map(
    currentProjects.map((project) => [project.id, project]),
  );
  return loadedProjects.map((loadedProject) => {
    const currentProject = currentByProject.get(loadedProject.id);
    const loadedEnvironments = loadedProject.environments ?? [];
    const deletedEnvironmentIds = new Set(
      loadedEnvironments
        .filter(
          (environment) =>
            !shouldPreserve(
              environment.id,
              snapshotGenerations.get(environment.id) ?? 0,
            ),
        )
        .map((environment) => environment.id),
    );
    const preservedEnvironments = (currentProject?.environments ?? []).filter(
      (environment) =>
        shouldPreserve(
          environment.id,
          snapshotGenerations.get(environment.id) ?? 0,
        ),
    );
    const preservedIds = new Set(
      preservedEnvironments.map((environment) => environment.id),
    );
    return {
      ...loadedProject,
      environments: [
        ...loadedEnvironments.filter(
          (environment) =>
            !deletedEnvironmentIds.has(environment.id) &&
            !preservedIds.has(environment.id),
        ),
        ...preservedEnvironments,
      ],
    };
  });
}
