import type { operations } from "@/lib/api.generated";
import type { DeployRecord, Environment, Service } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
import { releaseFromAPI } from "@/features/service/api";

export type ReleasePageResponse =
  operations["release.list"]["responses"][200]["content"]["application/json"];
export type ReleaseDetailResponse =
  operations["release.show"]["responses"][200]["content"]["application/json"];

export async function listAllReleases(
  environmentId: string,
  services: Service[],
  signal?: AbortSignal,
): Promise<DeployRecord[]> {
  const releases: DeployRecord[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment_id: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ReleasePageResponse>(
      `/releases?${query}`,
      200,
      { signal },
    );
    releases.push(
      ...(page.items ?? []).map((release) => releaseFromAPI(release, services)),
    );
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return releases;
}

export function projectReleaseSummary(
  deploys: DeployRecord[],
): Pick<Environment, "release" | "previousRelease" | "lastDeployAt"> {
  const activeTags = [
    ...new Set(
      deploys
        .filter((deploy) => deploy.status === "active")
        .map((deploy) => deploy.tag),
    ),
  ];
  const previousTags = [
    ...new Set(
      deploys
        .filter(
          (deploy) =>
            deploy.status === "superseded" && !activeTags.includes(deploy.tag),
        )
        .map((deploy) => deploy.tag),
    ),
  ];
  const latest = deploys.reduce<DeployRecord | undefined>(
    (current, candidate) =>
      !current || candidate.when > current.when ? candidate : current,
    undefined,
  );
  return {
    release:
      activeTags.length === 0
        ? "none"
        : activeTags.length === 1
          ? activeTags[0]
          : "mixed",
    previousRelease: previousTags.length === 1 ? previousTags[0] : undefined,
    lastDeployAt: latest?.when ?? "never",
  };
}

export function fetchReleaseDetail(
  id: string,
  signal?: AbortSignal,
): Promise<ReleaseDetailResponse> {
  return controllerRequest<ReleaseDetailResponse>(
    `/releases/${encodeURIComponent(id)}`,
    200,
    { signal },
  );
}
