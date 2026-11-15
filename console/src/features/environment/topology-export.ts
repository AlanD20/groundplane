import { currentServiceObservation } from "@/features/service/service-observation";
import { buildTopology, type TopologyLayer } from "./topology-model";

type Graph = ReturnType<typeof buildTopology>;
export type TopologyImageFormat = "svg" | "png";

const xml = (value: string) =>
  value.replace(
    /[&<>"']/g,
    (character) =>
      ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&apos;",
      })[character]!,
  );
const short = (value: string, length: number) =>
  value.length > length ? `${value.slice(0, length - 1)}…` : value;

/** Resolve the current theme to portable RGB, with no external styles or fonts. */
function palette(element: HTMLElement) {
  const probe = document.createElement("span");
  element.append(probe);
  const context = document
    .createElement("canvas")
    .getContext("2d", { willReadFrequently: true });
  if (!context) {
    probe.remove();
    throw new Error("Image export is unavailable in this browser.");
  }
  const read = (variable: string) => {
    probe.style.color = `var(--${variable})`;
    context.clearRect(0, 0, 1, 1);
    context.fillStyle = getComputedStyle(probe).color;
    context.fillRect(0, 0, 1, 1);
    const [r, g, b] = context.getImageData(0, 0, 1, 1).data;
    return `rgb(${r},${g},${b})`;
  };
  try {
    return {
      card: read("card"),
      accent: read("accent"),
      border: read("border"),
      text: read("foreground"),
      muted: read("muted-foreground"),
      primary: read("primary"),
      success: read("success"),
      warning: read("warning"),
      destructive: read("destructive"),
    };
  } finally {
    probe.remove();
  }
}

/** Export vector shapes rather than HTML foreignObjects, so PNG stays portable. */
export function topologySVG(
  graph: Graph,
  title: string,
  element: HTMLElement,
  now: number,
) {
  if (!graph.nodes.length) throw new Error("There are no resources to export.");
  const theme = palette(element);
  const colors: Record<TopologyLayer, string> = {
    Routes: theme.success,
    Dependencies: theme.primary,
    Backing: theme.warning,
    Storage: theme.muted,
  };
  const nodeColors = {
    Route: colors.Routes,
    Service: colors.Dependencies,
    Backing: colors.Backing,
    Volume: colors.Storage,
  };
  const text = (
    x: number,
    y: number,
    value: string,
    size = 12,
    color = theme.text,
    extra = "",
  ) =>
    `<text x="${x}" y="${y}" fill="${color}" font-size="${size}" ${extra}>${xml(value)}</text>`;
  const shapes: string[] = [];
  for (const lane of graph.lanes) {
    const ys = graph.nodes
      .filter((node) => node.zone === lane)
      .map((node) => graph.positions.get(node.id)!.y);
    const y = Math.min(...ys) - 32;
    shapes.push(
      `<g><title>${xml(lane)}</title><rect x="376" y="${y}" width="286" height="${Math.max(...ys) - y + 86}" rx="14" fill="${theme.accent}" stroke="${theme.border}"/>`,
      text(392, y + 20, short(lane, 35), 12, theme.muted),
      "</g>",
    );
  }
  for (const edge of graph.edges) {
    shapes.push(
      `<path d="${edge.path}" fill="none" stroke="${colors[edge.kind]}" stroke-width="1.5" marker-end="url(#export-${edge.kind})"><title>${xml(edge.label)}</title></path>`,
    );
  }
  for (const node of graph.nodes) {
    const { x, y } = graph.positions.get(node.id)!;
    shapes.push(
      `<g><title>${xml(`${node.label} · ${node.zone ?? node.detail}`)}</title><rect x="${x}" y="${y}" width="240" height="78" rx="8" fill="${theme.card}" stroke="${theme.border}"/><path d="M${x + 2} ${y + 8}V${y + 70}" stroke="${nodeColors[node.kind]}" stroke-width="4"/>`,
      text(x + 12, y + 17, node.kind.toUpperCase(), 10, nodeColors[node.kind]),
      text(
        x + 12,
        y + 39,
        short(node.label, 30),
        13,
        theme.text,
        'font-weight="600"',
      ),
      text(
        x + 12,
        y + 61,
        short(node.zone ?? node.detail, 38),
        10,
        theme.muted,
      ),
    );
    if (node.observation) {
      const state = currentServiceObservation(node.observation, now).state;
      const color =
        state === "healthy"
          ? theme.success
          : state === "failed"
            ? theme.destructive
            : ["degraded", "starting"].includes(state)
              ? theme.warning
              : theme.muted;
      const label =
        state === "running"
          ? "Running · unchecked"
          : state[0].toUpperCase() + state.slice(1);
      shapes.push(text(x + 228, y + 17, label, 10, color, 'text-anchor="end"'));
    }
    shapes.push("</g>");
  }
  const namespace = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(namespace, "svg");
  svg.setAttribute("xmlns", namespace);
  svg.setAttribute("font-family", "sans-serif");
  svg.innerHTML = `<title>${xml(title)}</title><defs>${Object.entries(colors)
    .map(
      ([kind, color]) =>
        `<marker id="export-${kind}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="${color}"/></marker>`,
    )
    .join("")}</defs><g>${shapes.join("")}</g>`;
  // Measure only drawn content, never the huge viewport, pan or zoom transform.
  svg.style.cssText =
    "position:fixed;left:-100000px;top:0;overflow:visible;pointer-events:none";
  element.append(svg);
  try {
    const bounds = svg.querySelector("g")!.getBBox();
    const width = Math.ceil(bounds.width + 32),
      height = Math.ceil(bounds.height + 32);
    svg.setAttribute(
      "viewBox",
      `${bounds.x - 16} ${bounds.y - 16} ${width} ${height}`,
    );
    svg.setAttribute("width", String(width));
    svg.setAttribute("height", String(height));
    svg.removeAttribute("style");
    const background = document.createElementNS(namespace, "rect");
    for (const [key, value] of Object.entries({
      x: bounds.x - 16,
      y: bounds.y - 16,
      width,
      height,
      fill: theme.card,
    }))
      background.setAttribute(key, String(value));
    svg.insertBefore(background, svg.querySelector("g"));
    return {
      source: new XMLSerializer().serializeToString(svg),
      width,
      height,
      background: theme.card,
    };
  } finally {
    svg.remove();
  }
}

export async function topologyImage(
  graph: Graph,
  title: string,
  element: HTMLElement,
  now: number,
  format: TopologyImageFormat,
) {
  const { source, width, height, background } = topologySVG(
    graph,
    title,
    element,
    now,
  );
  const vector = new Blob([source], { type: "image/svg+xml;charset=utf-8" });
  if (format === "svg") return vector;
  // Bound both dimensions and pixel area; large diagrams remain available as SVG.
  const scale = Math.min(
    1,
    16352 / (width - 32),
    16352 / (height - 32),
    Math.sqrt(31_000_000 / ((width - 32) * (height - 32))),
  );
  const canvas = document.createElement("canvas");
  canvas.width = Math.ceil((width - 32) * scale) + 32;
  canvas.height = Math.ceil((height - 32) * scale) + 32;
  const context = canvas.getContext("2d");
  if (!context)
    throw new Error("PNG export is unavailable. Choose SVG instead.");
  const url = URL.createObjectURL(vector);
  try {
    const image = new Image();
    await new Promise<void>((resolve, reject) => {
      image.onload = () => resolve();
      image.onerror = () =>
        reject(new Error("Unable to render PNG. Choose SVG instead."));
      image.src = url;
    });
    context.fillStyle = background;
    context.fillRect(0, 0, canvas.width, canvas.height);
    // Preserve 16 physical pixels of margin even when a huge PNG is scaled down.
    const inset = 16 * (1 - scale);
    context.drawImage(image, inset, inset, width * scale, height * scale);
    return await new Promise<Blob>((resolve, reject) =>
      canvas.toBlob(
        (blob) =>
          blob
            ? resolve(blob)
            : reject(new Error("Unable to create PNG. Choose SVG instead.")),
        "image/png",
      ),
    );
  } finally {
    URL.revokeObjectURL(url);
  }
}
