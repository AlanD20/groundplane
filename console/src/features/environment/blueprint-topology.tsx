import { useId, useMemo, useState } from "react";
import { TopologyCanvas } from "@/components/common/topology-canvas";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";
import {
  buildTopology,
  topologyLayers,
  type TopologyLayer,
} from "./topology-model";

const colors = {
  Routes: "var(--success)",
  Dependencies: "var(--primary)",
  Backing: "var(--warning)",
  Storage: "var(--muted-foreground)",
};
const nodeColors = {
  Route: colors.Routes,
  Service: colors.Dependencies,
  Backing: colors.Backing,
  Volume: colors.Storage,
};

export function BlueprintTopology({ env }: { env: Environment }) {
  const { backingProjects } = useStore();
  const graph = useMemo(
    () => buildTopology(env, backingProjects),
    [env, backingProjects],
  );
  const [selection, setSelection] = useState<string | null>(null);
  const selected = graph.nodes.some((n) => n.id === selection)
    ? selection
    : null;
  const [layers, setLayers] = useState<TopologyLayer[]>([...topologyLayers]);
  const [onlyRelated, setOnlyRelated] = useState(false);
  const marker = useId().replaceAll(":", "");
  const edges = graph.edges.filter(
    (e) =>
      layers.includes(e.kind) &&
      (!onlyRelated || !selected || e.from === selected || e.to === selected),
  );
  const highlighted = selected
    ? edges.filter((e) => e.from === selected || e.to === selected)
    : [];
  const related = new Set([
    selected,
    ...highlighted.flatMap((e) => [e.from, e.to]),
  ]);
  const active = graph.nodes.find((n) => n.id === selected);
  if (!graph.nodes.length)
    return (
      <p className="rounded-lg border border-border bg-card p-6 text-sm text-muted-foreground">
        No Services, Routes or connections yet.
      </p>
    );
  return (
    <section className="space-y-3" aria-label="Blueprint topology">
      <div className="flex flex-wrap items-center gap-4">
        {topologyLayers.map((layer) => (
          <label key={layer} className="flex items-center gap-2 text-xs">
            <Checkbox
              checked={layers.includes(layer)}
              onChange={(event) =>
                setLayers((current) =>
                  event.target.checked
                    ? [...current, layer]
                    : current.filter((l) => l !== layer),
                )
              }
            />
            {layer}
            <span className="text-muted-foreground">
              {graph.edges.filter((e) => e.kind === layer).length}
            </span>
          </label>
        ))}
        <label className="flex items-center gap-2 text-xs">
          <Checkbox
            checked={onlyRelated}
            disabled={!selected}
            onChange={(event) => setOnlyRelated(event.target.checked)}
          />
          Selected connections only
        </label>
        <span role="status" className="text-xs text-muted-foreground">
          {edges.length} / {graph.edges.length} connections
        </span>
        {selected && (
          <Button size="xs" variant="ghost" onClick={() => setSelection(null)}>
            Clear selection
          </Button>
        )}
      </div>
      <TopologyCanvas width={graph.width} height={graph.height}>
        <defs>
          {topologyLayers.map((layer) => (
            <marker
              key={layer}
              id={`${marker}-${layer}`}
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M0 0L10 5L0 10z" fill={colors[layer]} />
            </marker>
          ))}
          <marker
            id={`${marker}-selected`}
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="6"
            markerHeight="6"
            orient="auto-start-reverse"
          >
            <path d="M0 0L10 5L0 10z" fill="var(--primary)" />
          </marker>
        </defs>
        {graph.lanes.map((lane) => {
          const ys = graph.nodes
            .filter((n) => n.zone === lane)
            .map((n) => graph.positions.get(n.id)!.y);
          const y = Math.min(...ys) - 32;
          return (
            <g key={lane}>
              <rect
                x="376"
                y={y}
                width="286"
                height={Math.max(...ys) - y + 86}
                rx="14"
                fill="var(--accent)"
                stroke="var(--border)"
              />
              <text
                x="392"
                y={y + 20}
                fill="var(--muted-foreground)"
                fontSize="12"
              >
                <title>{lane}</title>
                {lane.length > 35 ? `${lane.slice(0, 32)}…` : lane}
              </text>
            </g>
          );
        })}
        <g opacity={selected ? 0.12 : 0.7} aria-hidden="true">
          {edges.map((edge) => (
            <path
              key={edge.id}
              d={edge.path}
              fill="none"
              stroke={colors[edge.kind]}
              strokeWidth="1.5"
              markerEnd={`url(#${marker}-${edge.kind})`}
            />
          ))}
        </g>
        {/* Draw complete selected paths last, above every dimmed crossing. */}
        <g aria-label="Highlighted connections">
          {highlighted.map((edge) => (
            <g key={edge.id} data-highlighted-connection>
              <path
                d={edge.path}
                fill="none"
                stroke="var(--card)"
                strokeWidth="7"
                strokeLinejoin="round"
              />
              <path
                d={edge.path}
                fill="none"
                stroke="var(--primary)"
                strokeWidth="3"
                strokeLinejoin="round"
                markerEnd={`url(#${marker}-selected)`}
              />
              <title>{`${graph.nodes.find((n) => n.id === edge.from)?.label} → ${graph.nodes.find((n) => n.id === edge.to)?.label}: ${edge.label}`}</title>
            </g>
          ))}
        </g>
        {graph.nodes.map((node) => {
          const p = graph.positions.get(node.id)!;
          return (
            <foreignObject
              key={node.id}
              x={p.x}
              y={p.y}
              width="240"
              height="78"
              opacity={!selected || related.has(node.id) ? 1 : 0.4}
            >
              <button
                type="button"
                aria-pressed={selected === node.id}
                onClick={() =>
                  setSelection(selected === node.id ? null : node.id)
                }
                className="h-[72px] w-full cursor-pointer rounded-lg border bg-card px-3 py-2 text-left text-foreground outline-none focus-visible:ring-2 focus-visible:ring-ring"
                style={{
                  borderColor:
                    selected === node.id ? "var(--primary)" : "var(--border)",
                  borderLeftWidth: 4,
                  borderLeftColor: nodeColors[node.kind],
                }}
              >
                <span
                  className="block text-[10px] uppercase tracking-wider"
                  style={{ color: nodeColors[node.kind] }}
                >
                  {node.kind}
                </span>
                <strong className="block truncate text-sm" title={node.label}>
                  {node.label}
                </strong>
                <span
                  className="block truncate text-[10px] text-muted-foreground"
                  title={node.zone ?? node.detail}
                >
                  {node.zone ?? node.detail}
                </span>
              </button>
            </foreignObject>
          );
        })}
      </TopologyCanvas>
      {active && (
        <div className="space-y-3 rounded-lg border border-border bg-card p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <strong className="break-all text-sm">{active.label}</strong>
            <Button
              size="xs"
              variant="ghost"
              onClick={() => setSelection(null)}
            >
              Close details
            </Button>
          </div>
          <p className="break-all text-xs text-muted-foreground">
            {active.detail}
          </p>
          <div className="grid gap-2 text-xs md:grid-cols-2">
            {highlighted.map((edge) => (
              <div
                key={edge.id}
                className="min-w-0 rounded-md border border-border p-2 [overflow-wrap:anywhere]"
              >
                <span>
                  {graph.nodes.find((n) => n.id === edge.from)?.label} →{" "}
                  {graph.nodes.find((n) => n.id === edge.to)?.label}
                </span>
                <span className="mt-1 block text-muted-foreground">
                  {edge.kind} · {edge.label}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </section>
  );
}
