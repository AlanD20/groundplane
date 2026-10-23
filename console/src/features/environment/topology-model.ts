import type { Environment, Project } from "@/lib/types";
import type { ServiceObservation } from "@/features/service/service-observation";
export const topologyLayers = [
  "Routes",
  "Dependencies",
  "Backing",
  "Storage",
] as const;
export type TopologyLayer = (typeof topologyLayers)[number];
export type TopologyNode = {
  id: string;
  label: string;
  kind: "Service" | "Route" | "Backing" | "Volume";
  detail: string;
  zone?: string;
  observation?: ServiceObservation;
  backingProjectId?: string;
};
export type TopologyEdge = {
  from: string;
  to: string;
  label: string;
  kind: TopologyLayer;
};
export function buildTopology(env: Environment, backingProjects: Project[]) {
  const nodes: TopologyNode[] = [];
  const edges: TopologyEdge[] = [];
  const serviceFor = (ref: string) =>
    env.services.find((s) => s.id === ref || s.name === ref);
  env.services.forEach((s) => {
    nodes.push({
      id: s.id,
      label: s.name,
      kind: "Service",
      observation: s.observation,
      detail: s.image,
      zone:
        s.zones
          .map(
            (ref) =>
              env.zones.find((zone) => zone.id === ref || zone.name === ref)
                ?.name ?? ref,
          )
          .sort()
          .join(", ") || "No Zone",
    });
    s.dependsOn.forEach((ref) => {
      const target = serviceFor(ref);
      if (target)
        edges.push({
          from: s.id,
          to: target.id,
          label: "depends on",
          kind: "Dependencies",
        });
    });
  });
  env.routes.forEach((r) => {
    nodes.push({
      id: r.id,
      label: r.host + r.path,
      kind: "Route",
      detail: `${r.exposure} · port ${r.targetPort}`,
    });
    edges.push({
      from: r.id,
      to: r.targetServiceId,
      label: "routes to",
      kind: "Routes",
    });
  });
  env.attaches.forEach((a) => {
    const id = `backing:${a.backingServiceId}`;
    const backing = backingProjects.find((p) => p.id === a.projectId);
    const service = backing?.environments
      ?.flatMap((environment) => environment.services)
      .find((candidate) => candidate.id === a.backingServiceId);
    if (!nodes.some((n) => n.id === id))
      nodes.push({
        id,
        label: backing?.name ?? a.projectId,
        kind: "Backing",
        backingProjectId: a.projectId,
        detail: "Shared backing service",
        observation: service?.observation ?? { state: "unavailable" },
      });
    edges.push({ from: a.serviceId, to: id, label: a.name, kind: "Backing" });
  });
  env.services.forEach((s) =>
    s.mounts.forEach((m) => {
      if (m.type !== "volume") return;
      const id = `volume:${m.volume}`;
      if (!nodes.some((n) => n.id === id))
        nodes.push({
          id,
          label:
            env.volumes.find(
              (v) =>
                v.id === m.volume || v.slug === m.volume || v.key === m.volume,
            )?.slug ?? m.volume,
          kind: "Volume",
          detail: "Persistent volume",
        });
      edges.push({ from: s.id, to: id, label: m.mount, kind: "Storage" });
    }),
  );

  const lanes = [
    ...new Set(nodes.filter((n) => n.kind === "Service").map((n) => n.zone!)),
  ];
  const positions = new Map<string, { x: number; y: number }>();
  let y = 80;
  for (const lane of lanes) {
    for (const node of nodes.filter((n) => n.zone === lane)) {
      positions.set(node.id, { x: 400, y });
      y += 100;
    }
    y += 64;
  }
  for (const kind of ["Route", "Backing", "Volume"] as const) {
    const offset =
      kind === "Volume" ? nodes.filter((n) => n.kind === "Backing").length : 0;
    nodes
      .filter((n) => n.kind === kind)
      .forEach((n, i) =>
        positions.set(n.id, {
          x: kind === "Route" ? 40 : 1060,
          y: 80 + (i + offset) * 120,
        }),
      );
  }
  const width = Math.max(
    1060,
    ...[...positions.values()].map((p) => p.x + 290),
  );
  const height = Math.max(
    540,
    ...[...positions.values()].map((p) => p.y + 120),
  );
  const validEdges = edges.filter(
    (e) => positions.has(e.from) && positions.has(e.to),
  );
  // Each edge owns its complete route. Shared coordinates bundle lines without
  // sharing highlight state, so selection cannot illuminate unrelated branches.
  const keys = [...new Set(validEdges.map((e) => e.kind + ":" + e.to))];
  const paths = validEdges.map((edge, index) => {
    const group = keys.indexOf(edge.kind + ":" + edge.to);
    const source = positions.get(edge.from)!;
    const target = positions.get(edge.to)!;
    const trunk =
      edge.kind === "Routes"
        ? 310 + (group % 5) * 14
        : edge.kind === "Dependencies"
          ? 700 + (group % 8) * 22
          : 880 + (group % 6) * 24;
    const targetX = edge.kind === "Dependencies" ? target.x + 240 : target.x;
    return {
      ...edge,
      id: `${edge.from}:${edge.to}:${index}`,
      path: `M${source.x + 240},${source.y + 36} H${trunk} V${target.y + 36} H${targetX}`,
    };
  });
  return { nodes, edges: paths, positions, lanes, width, height };
}
