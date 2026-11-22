import { cn } from "@/lib/utils";
import {
  useCallback,
  useEffect,
  useId,
  useMemo,
  useRef,
  useState,
} from "react";
import type {
  CSSProperties,
  KeyboardEvent,
  MouseEvent,
  PointerEvent,
} from "react";
import {
  averagePath,
  bucketIndexAtTime,
  chartDomainMaximum,
  chartPlot,
  chartX,
  chartY,
  frozenSeries,
  rangeBandPath,
} from "./monitoring-chart-geometry";
import type {
  MonitoringChartBucketPoint,
  MonitoringChartSeries,
  MonitoringChartWindow,
} from "./monitoring-chart-geometry";

export type {
  MonitoringChartBucketPoint,
  MonitoringChartSeries,
  MonitoringChartWindow,
} from "./monitoring-chart-geometry";

export type MonitoringChartProps = {
  readonly series: readonly MonitoringChartSeries[];
  readonly rangeLabel: string;
  readonly window: MonitoringChartWindow;
  readonly scopeKey: string;
  readonly scopedLabel: string;
  readonly height?: number;
  readonly className?: string;
};

type Interaction = {
  readonly index: number;
  readonly pinned: boolean;
  readonly series: readonly MonitoringChartSeries[];
  readonly window: MonitoringChartWindow;
};

type PendingPointer = {
  readonly clientX: number;
  readonly left: number;
  readonly width: number;
};

const viewBoxWidth = 800;
const viewBoxHeight = 248;

const valueFormatter = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 2,
});

const timestampFormatter = new Intl.DateTimeFormat(undefined, {
  timeZone: "UTC",
  day: "2-digit",
  month: "short",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});

const axisTimestampFormatter = new Intl.DateTimeFormat(undefined, {
  timeZone: "UTC",
  day: "2-digit",
  month: "short",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});

function pointCenter(point: MonitoringChartBucketPoint): number {
  return point.from + (point.to - point.from) / 2;
}

function formatValue(value: number | null, unit: string): string {
  if (value === null || !Number.isFinite(value)) return "Unavailable";
  return `${valueFormatter.format(value)} ${unit}`;
}

function formatCoverage(valid: number, expected: number): string {
  if (expected === 0) return "Not applicable";
  return `${valueFormatter.format(valid)} / ${valueFormatter.format(expected)}`;
}

function seriesDash(index: number): string | undefined {
  return [undefined, "7 4", "2 3", "10 3 2 3"][index % 4];
}

function selectionIdentity(
  scopeKey: string,
  scopedLabel: string,
  rangeLabel: string,
  series: readonly MonitoringChartSeries[],
): string {
  return `${scopeKey}\u0000${scopedLabel}\u0000${rangeLabel}\u0000${series
    .map(
      (item) =>
        `${item.id}\u0000${item.label}\u0000${item.unit}\u0000${item.color}`,
    )
    .join("\u0001")}`;
}

function pointForTime(
  item: MonitoringChartSeries,
  time: number,
): MonitoringChartBucketPoint | null {
  const index = bucketIndexAtTime(item.points, time);
  if (index === null) return null;
  const point = item.points[index];
  return time >= point.from && time < point.to ? point : null;
}

function MonitoringTooltip({
  id,
  series,
  selected,
  pinned,
}: {
  readonly id: string;
  readonly series: readonly MonitoringChartSeries[];
  readonly selected: MonitoringChartBucketPoint;
  readonly pinned: boolean;
}) {
  const time = pointCenter(selected);
  return (
    <div
      id={id}
      role="tooltip"
      className={cn(
        "absolute left-2 right-2 top-2 z-20 max-h-[min(28rem,calc(100dvh-2rem))] overflow-y-auto",
        "rounded-lg border border-border bg-popover p-3 text-popover-foreground shadow-lg",
        "sm:right-auto sm:w-[26rem]",
      )}
      onClick={(event) => event.stopPropagation()}
      onPointerMove={(event) => event.stopPropagation()}
    >
      <div className="flex items-start justify-between gap-3 border-b border-border pb-2">
        <div className="min-w-0">
          <p className="text-xs font-semibold text-primary">
            {pinned ? "Pinned bucket" : "Selected bucket"}
          </p>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            {timestampFormatter.format(selected.from)} –{" "}
            {timestampFormatter.format(selected.to)} UTC
          </p>
        </div>
        <span className="shrink-0 rounded-md bg-muted px-2 py-1 text-xs text-muted-foreground">
          {pinned ? "Enter to unpin" : "Enter to pin"}
        </span>
      </div>
      <div className="divide-y divide-border">
        {series.map((item) => {
          const point = pointForTime(item, time);
          return (
            <section key={item.id} className="space-y-2 py-3">
              <div className="flex items-center gap-2 text-xs font-semibold">
                <span
                  aria-hidden="true"
                  className="size-2 rounded-full"
                  style={{ backgroundColor: item.color }}
                />
                {item.label}
              </div>
              <dl className="grid grid-cols-3 gap-2 text-xs">
                {[
                  ["Average", point?.average ?? null],
                  ["Minimum", point?.minimum ?? null],
                  ["Maximum", point?.maximum ?? null],
                ].map(([label, value]) => (
                  <div key={String(label)} className="min-w-0">
                    <dt className="text-muted-foreground">{label}</dt>
                    <dd className="mt-0.5 truncate font-mono text-xs tabular-nums">
                      {formatValue(value as number | null, item.unit)}
                    </dd>
                  </div>
                ))}
              </dl>
              <dl className="grid gap-2 text-xs text-muted-foreground sm:grid-cols-2">
                <div className="flex justify-between gap-3">
                  <dt>Samples</dt>
                  <dd className="font-mono tabular-nums text-foreground">
                    {formatCoverage(
                      point?.validSamples ?? 0,
                      point?.expectedSamples ?? 0,
                    )}
                  </dd>
                </div>
                <div className="flex justify-between gap-3">
                  <dt>Contributions</dt>
                  <dd className="font-mono tabular-nums text-foreground">
                    {formatCoverage(
                      point?.validContributions ?? 0,
                      point?.expectedContributions ?? 0,
                    )}
                  </dd>
                </div>
              </dl>
            </section>
          );
        })}
      </div>
    </div>
  );
}

export function MonitoringChart({
  series,
  rangeLabel,
  window,
  scopeKey,
  scopedLabel,
  height = 304,
  className,
}: MonitoringChartProps) {
  const tooltipId = `${useId().replaceAll(":", "")}-monitoring-tooltip`;
  const [interaction, setInteraction] = useState<Interaction | null>(null);
  const frame = useRef<number | null>(null);
  const pendingPointer = useRef<PendingPointer | null>(null);
  const identity = selectionIdentity(scopeKey, scopedLabel, rangeLabel, series);

  useEffect(() => {
    setInteraction(null);
  }, [identity]);

  useEffect(
    () => () => {
      if (frame.current !== null) cancelAnimationFrame(frame.current);
    },
    [],
  );

  const displayedSeries = interaction?.series ?? series;
  const displayedWindow = interaction?.window ?? window;
  const reference = displayedSeries.find((item) => item.points.length > 0);
  const maximum = useMemo(
    () => chartDomainMaximum(displayedSeries),
    [displayedSeries],
  );
  const geometry = useMemo(
    () =>
      displayedSeries.map((item, index) => ({
        item,
        dash: seriesDash(index),
        average: averagePath(item.points, displayedWindow, maximum),
        band: rangeBandPath(item.points, displayedWindow, maximum),
      })),
    [displayedSeries, displayedWindow, maximum],
  );
  const hasValues = displayedSeries.some((item) =>
    item.points.some((point) => point.average !== null),
  );
  const selected =
    reference && interaction
      ? (reference.points[interaction.index] ?? null)
      : null;

  const dismiss = useCallback(() => {
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = null;
    pendingPointer.current = null;
    setInteraction(null);
  }, []);

  const selectAtClientX = useCallback(
    (clientX: number, left: number, width: number, pinned?: boolean) => {
      const activeSeries = interaction?.series ?? series;
      const activeReference = activeSeries.find(
        (item) => item.points.length > 0,
      );
      if (!activeReference || width <= 0) return;
      const svgX = ((clientX - left) / width) * viewBoxWidth;
      const plotFraction = Math.min(
        1,
        Math.max(
          0,
          (svgX - chartPlot.left) / (chartPlot.right - chartPlot.left),
        ),
      );
      const activeWindow = interaction?.window ?? window;
      const time =
        activeWindow.from +
        plotFraction * (activeWindow.to - activeWindow.from);
      const index = bucketIndexAtTime(activeReference.points, time);
      if (index === null) return;
      setInteraction((current) => ({
        index,
        pinned: pinned ?? current?.pinned ?? false,
        series: current?.series ?? frozenSeries(series),
        window: current?.window ?? { ...window },
      }));
    },
    [interaction?.series, interaction?.window, series, window],
  );

  const handlePointerMove = (event: PointerEvent<HTMLDivElement>) => {
    if (interaction?.pinned || event.pointerType === "touch") return;
    const bounds = event.currentTarget.getBoundingClientRect();
    pendingPointer.current = {
      clientX: event.clientX,
      left: bounds.left,
      width: bounds.width,
    };
    if (frame.current !== null) return;
    frame.current = requestAnimationFrame(() => {
      frame.current = null;
      const pending = pendingPointer.current;
      pendingPointer.current = null;
      if (pending)
        selectAtClientX(pending.clientX, pending.left, pending.width);
    });
  };

  const handleClick = (event: MouseEvent<HTMLDivElement>) => {
    if (frame.current !== null) cancelAnimationFrame(frame.current);
    frame.current = null;
    pendingPointer.current = null;
    const bounds = event.currentTarget.getBoundingClientRect();
    selectAtClientX(
      event.clientX,
      bounds.left,
      bounds.width,
      !interaction?.pinned,
    );
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!reference) return;
    if (event.key === "Escape") {
      dismiss();
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const index =
        interaction?.index ?? Math.floor(reference.points.length / 2);
      setInteraction({
        index,
        pinned: !interaction?.pinned,
        series: interaction?.series ?? frozenSeries(series),
        window: interaction?.window ?? { ...window },
      });
      return;
    }
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const current =
      interaction?.index ?? Math.floor(reference.points.length / 2);
    const direction = event.key === "ArrowRight" ? 1 : -1;
    setInteraction({
      index: Math.min(
        reference.points.length - 1,
        Math.max(0, current + direction),
      ),
      pinned: interaction?.pinned ?? false,
      series: interaction?.series ?? frozenSeries(series),
      window: interaction?.window ?? { ...window },
    });
  };

  if (!reference) {
    return (
      <div
        role="status"
        className={cn(
          "flex min-h-56 items-center justify-center rounded-lg border border-dashed border-border bg-surface/40",
          "text-sm text-muted-foreground",
          className,
        )}
      >
        No monitoring history is available for this window.
      </div>
    );
  }

  const selectedX = selected
    ? chartX(pointCenter(selected), displayedWindow)
    : null;
  const rootStyle = { height: Math.max(280, height) } satisfies CSSProperties;

  return (
    <div
      className={cn(
        "relative min-w-0 overflow-visible rounded-lg border border-border bg-surface/30",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2",
        className,
      )}
      style={rootStyle}
      role="group"
      tabIndex={0}
      aria-label={`${scopedLabel} ${rangeLabel} monitoring chart. Use the arrow keys to inspect buckets, Enter to pin, and Escape to dismiss.`}
      aria-describedby={selected ? tooltipId : undefined}
      onKeyDown={handleKeyDown}
      onPointerMove={handlePointerMove}
      onPointerLeave={() => {
        if (!interaction?.pinned) dismiss();
      }}
      onClick={handleClick}
      onBlur={() => {
        if (!interaction?.pinned) dismiss();
      }}
    >
      <div className="absolute left-3 right-3 top-2 z-10 flex flex-wrap items-center justify-between gap-2 text-xs">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1">
          {displayedSeries.map((item, index) => (
            <span key={item.id} className="inline-flex items-center gap-1.5">
              <svg aria-hidden="true" className="h-2 w-5" viewBox="0 0 20 8">
                <line
                  x1="1"
                  x2="19"
                  y1="4"
                  y2="4"
                  stroke={item.color}
                  strokeWidth="2"
                  strokeDasharray={seriesDash(index)}
                />
              </svg>
              {item.label} · {item.unit}
            </span>
          ))}
        </div>
        <span className="text-muted-foreground">
          {rangeLabel} · line average · band min–max
        </span>
      </div>
      <svg
        className="size-full"
        viewBox={`0 0 ${viewBoxWidth} ${viewBoxHeight}`}
        preserveAspectRatio="none"
        aria-hidden="true"
      >
        {[0, 0.25, 0.5, 0.75, 1].map((fraction) => {
          const y =
            chartPlot.top + fraction * (chartPlot.bottom - chartPlot.top);
          const value = maximum * (1 - fraction);
          return (
            <g key={fraction}>
              <line
                x1={chartPlot.left}
                x2={chartPlot.right}
                y1={y}
                y2={y}
                stroke="var(--border)"
                strokeDasharray="3 5"
                vectorEffect="non-scaling-stroke"
              />
              <text
                x={chartPlot.left - 8}
                y={y + 4}
                textAnchor="end"
                fill="var(--muted-foreground)"
                fontSize="11"
              >
                {valueFormatter.format(value)}
              </text>
            </g>
          );
        })}
        {geometry.map(({ item, dash, band, average }) => (
          <g key={item.id}>
            {band && <path d={band} fill={item.color} opacity={0.1} />}
            {average && (
              <path
                d={average}
                fill="none"
                stroke={item.color}
                strokeWidth={2}
                strokeDasharray={dash}
                strokeLinecap="round"
                strokeLinejoin="round"
                vectorEffect="non-scaling-stroke"
              />
            )}
          </g>
        ))}
        {selectedX !== null && (
          <g>
            <line
              x1={selectedX}
              x2={selectedX}
              y1={chartPlot.top}
              y2={chartPlot.bottom}
              stroke="var(--foreground)"
              strokeDasharray="3 4"
              opacity={0.55}
              vectorEffect="non-scaling-stroke"
            />
            {displayedSeries.map((item) => {
              const point = selected
                ? pointForTime(item, pointCenter(selected))
                : null;
              if (point?.average === null || point?.average === undefined)
                return null;
              return (
                <circle
                  key={item.id}
                  cx={selectedX}
                  cy={chartY(point.average, maximum)}
                  r={4}
                  fill={item.color}
                  stroke="var(--card)"
                  strokeWidth={2}
                  vectorEffect="non-scaling-stroke"
                />
              );
            })}
          </g>
        )}
        {[
          displayedWindow.from,
          (displayedWindow.from + displayedWindow.to) / 2,
          displayedWindow.to,
        ].map((time, index) => (
          <text
            key={time}
            x={chartX(time, displayedWindow)}
            y={viewBoxHeight - 8}
            textAnchor={index === 0 ? "start" : index === 2 ? "end" : "middle"}
            fill="var(--muted-foreground)"
            fontSize="11"
          >
            {axisTimestampFormatter.format(time)}
          </text>
        ))}
      </svg>
      {!hasValues && (
        <div className="pointer-events-none absolute inset-0 flex items-center justify-center text-sm text-muted-foreground">
          No samples were recorded in this window.
        </div>
      )}
      {selected && interaction && (
        <MonitoringTooltip
          id={tooltipId}
          series={displayedSeries}
          selected={selected}
          pinned={interaction.pinned}
        />
      )}
      <p className="sr-only">
        Lines show bucket averages. Shaded bands show recorded minimum and
        maximum values. Missing buckets are gaps, not zero values.
      </p>
    </div>
  );
}
