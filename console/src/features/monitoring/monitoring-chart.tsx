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
  readonly height?: CSSProperties["height"];
  readonly className?: string;
  readonly timeZone?: string;
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

const viewBoxHeight = 248;
const plotWidth = chartPlot.right - chartPlot.left;
const valueTickCount = 4;

const valueFormatter = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 2,
});

const timestampFormat = {
  day: "2-digit",
  month: "short",
  year: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
  timeZoneName: "shortOffset",
} satisfies Intl.DateTimeFormatOptions;

const axisDateFormat = {
  day: "2-digit",
  month: "short",
} satisfies Intl.DateTimeFormatOptions;

const axisTimeFormat = {
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
} satisfies Intl.DateTimeFormatOptions;

function chartTimeTicks(window: MonitoringChartWindow, timeZone: string) {
  const minute = 60_000;
  const hour = 60 * minute;
  const span = window.to - window.from;
  if (!(span > 0)) return [];
  const step =
    span <= hour
      ? 15 * minute
      : span <= 6 * hour
        ? hour
        : span <= 24 * hour
          ? 4 * hour
          : 24 * hour;
  const formatter = new Intl.DateTimeFormat(undefined, {
    ...(span <= 24 * hour ? axisTimeFormat : axisDateFormat),
    timeZone,
  });
  const ticks = [];
  for (
    let time = Math.ceil(window.from / step) * step;
    time <= window.to;
    time += step
  ) {
    ticks.push({
      time,
      label: formatter.format(time),
      fraction: (time - window.from) / span,
    });
  }
  return ticks;
}

function readableMaximum(maximum: number) {
  const interval = maximum / valueTickCount;
  const order = 10 ** Math.floor(Math.log10(interval));
  const step =
    [1, 2, 2.5, 5, 10].find((value) => value * order >= interval) ?? 10;
  return step * order * valueTickCount;
}

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

function MonitoringBucketDetails({
  id,
  series,
  bucket,
  label,
  pinned,
  timeZone,
}: {
  readonly id: string;
  readonly series: readonly MonitoringChartSeries[];
  readonly bucket: MonitoringChartBucketPoint;
  readonly label: string;
  readonly pinned: boolean;
  readonly timeZone: string;
}) {
  const time = pointCenter(bucket);
  return (
    <div id={id} className="min-w-0 border-t border-border bg-card px-3 py-2">
      <div className="flex min-w-0 items-center gap-2 text-xs">
        <span className="shrink-0 font-semibold text-primary">{label}</span>
        <time
          className="min-w-0 truncate text-muted-foreground"
          dateTime={new Date(bucket.from).toISOString()}
        >
          {new Intl.DateTimeFormat(undefined, {
            ...timestampFormat,
            timeZone,
          }).formatRange(bucket.from, bucket.to)}
        </time>
        <span className="ml-auto hidden shrink-0 text-muted-foreground sm:inline">
          {pinned ? "Enter to unpin" : "Enter to pin"}
        </span>
      </div>
      <div className="mt-1 overflow-x-auto">
        <table className="w-full min-w-[31rem] text-left text-xs tabular-nums">
          <thead className="text-muted-foreground">
            <tr>
              <th scope="col" className="py-1 font-medium">
                Series
              </th>
              {["Average", "Min", "Max", "Samples", "Contributors"].map(
                (name) => (
                  <th scope="col" key={name} className="px-1 py-1 font-medium">
                    {name}
                  </th>
                ),
              )}
            </tr>
          </thead>
          <tbody>
            {series.map((item) => {
              const point = pointForTime(item, time);
              return (
                <tr key={item.id} className="border-t border-border">
                  <th scope="row" className="py-1 font-medium">
                    <span className="flex items-center gap-1.5 whitespace-nowrap">
                      <span
                        aria-hidden="true"
                        className="size-2 rounded-full"
                        style={{ backgroundColor: item.color }}
                      />
                      {item.label}
                    </span>
                  </th>
                  {point?.average != null ? (
                    <>
                      {[point.average, point.minimum, point.maximum].map(
                        (value, index) => (
                          <td
                            key={index}
                            className="whitespace-nowrap px-1 py-1"
                          >
                            {formatValue(value, item.unit)}
                          </td>
                        ),
                      )}
                    </>
                  ) : (
                    <td colSpan={3} className="px-1 py-1 text-muted-foreground">
                      No samples in this interval
                    </td>
                  )}
                  <td className="whitespace-nowrap px-1 py-1">
                    {formatCoverage(
                      point?.validSamples ?? 0,
                      point?.expectedSamples ?? 0,
                    )}
                  </td>
                  <td className="whitespace-nowrap px-1 py-1">
                    {formatCoverage(
                      point?.validContributions ?? 0,
                      point?.expectedContributions ?? 0,
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
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
  timeZone = "UTC",
}: MonitoringChartProps) {
  const detailsId = `${useId().replaceAll(":", "")}-monitoring-bucket`;
  const [interaction, setInteraction] = useState<Interaction | null>(null);
  const frame = useRef<number | null>(null);
  const pendingPointer = useRef<PendingPointer | null>(null);
  const plot = useRef<SVGSVGElement>(null);
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
  const scale = useMemo(() => {
    const maximum = chartDomainMaximum(displayedSeries);
    const unit = displayedSeries[0]?.unit;
    const useGiB =
      maximum >= 1024 && displayedSeries.every((item) => item.unit === "MiB");
    const factor = useGiB ? 1024 : 1;
    const top = readableMaximum(maximum / factor);
    return {
      unit: useGiB ? "GiB" : unit,
      maximum: top * factor,
      labels: Array.from(
        { length: valueTickCount + 1 },
        (_, index) => top * (1 - index / valueTickCount),
      ),
    };
  }, [displayedSeries]);
  const valueTicks = scale.labels.map((value, index) => ({
    value,
    y:
      chartPlot.top +
      (index / valueTickCount) * (chartPlot.bottom - chartPlot.top),
  }));
  const timeTicks = useMemo(
    () => chartTimeTicks(displayedWindow, timeZone),
    [displayedWindow, timeZone],
  );
  const geometry = useMemo(
    () =>
      displayedSeries.map((item, index) => ({
        item,
        dash: seriesDash(index),
        average: averagePath(item.points, displayedWindow, scale.maximum),
        band: rangeBandPath(item.points, displayedWindow, scale.maximum),
      })),
    [displayedSeries, displayedWindow, scale.maximum],
  );
  const latestBucket = useMemo(() => {
    let latest: MonitoringChartBucketPoint | null = null;
    for (const item of displayedSeries) {
      for (const point of item.points) {
        if (point.average !== null && (!latest || point.from > latest.from))
          latest = point;
      }
    }
    return latest;
  }, [displayedSeries]);
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
      const plotFraction = Math.min(1, Math.max(0, (clientX - left) / width));
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
    const bounds = plot.current?.getBoundingClientRect();
    if (!bounds || event.clientX < bounds.left || event.clientX > bounds.right)
      return;
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
    const bounds = plot.current?.getBoundingClientRect();
    if (!bounds || event.clientX < bounds.left || event.clientX > bounds.right)
      return;
    selectAtClientX(
      event.clientX,
      bounds.left,
      bounds.width,
      !interaction?.pinned,
    );
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!reference || !latestBucket) return;
    if (event.key === "Escape") {
      dismiss();
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      const index =
        interaction?.index ??
        bucketIndexAtTime(reference.points, pointCenter(latestBucket));
      if (index === null) return;
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
      interaction?.index ??
      bucketIndexAtTime(reference.points, pointCenter(latestBucket));
    if (current === null) return;
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

  if (!reference || !latestBucket) {
    return (
      <div
        role="status"
        className={cn(
          "flex min-h-56 items-center justify-center rounded-lg border border-dashed border-border bg-surface/40",
          "text-sm text-muted-foreground",
          className,
        )}
      >
        No samples were recorded in this window.
      </div>
    );
  }

  const selectedX = selected
    ? chartX(pointCenter(selected), displayedWindow)
    : null;
  const rootStyle = { height, minHeight: 280 } satisfies CSSProperties;

  return (
    <div
      className={cn(
        "min-w-0 overflow-hidden rounded-lg border border-border bg-surface/30",
        className,
      )}
      onPointerLeave={() => {
        if (!interaction?.pinned) dismiss();
      }}
    >
      <div
        className={cn(
          "relative min-w-0 overflow-hidden",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-inset",
        )}
        style={rootStyle}
        role="group"
        tabIndex={0}
        aria-label={`${scopedLabel} ${rangeLabel} monitoring chart. Use the arrow keys to inspect buckets, Enter to pin, and Escape to dismiss.`}
        aria-describedby={detailsId}
        onKeyDown={handleKeyDown}
        onPointerMove={handlePointerMove}
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
            {timeZone} · Line average · Band min–max
          </span>
        </div>
        <div
          aria-hidden="true"
          className="pointer-events-none absolute inset-y-0 left-0 w-12 text-right text-xs tabular-nums text-muted-foreground"
        >
          <span className="absolute right-0 top-[10%]">{scale.unit}</span>
          {valueTicks.map(({ value, y }) => (
            <span
              className="absolute right-0 -translate-y-1/2"
              style={{ top: `${(y / viewBoxHeight) * 100}%` }}
              key={y}
            >
              {valueFormatter.format(value)}
            </span>
          ))}
        </div>
        <div className="absolute inset-y-0 left-14 right-5">
          <svg
            ref={plot}
            className="size-full"
            viewBox={`${chartPlot.left} 0 ${plotWidth} ${viewBoxHeight}`}
            preserveAspectRatio="none"
            aria-hidden="true"
          >
            {valueTicks.map(({ y }) => (
              <line
                key={y}
                x1={chartPlot.left}
                x2={chartPlot.right}
                y1={y}
                y2={y}
                stroke="var(--border)"
                strokeDasharray="3 5"
                vectorEffect="non-scaling-stroke"
              />
            ))}
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
                      cy={chartY(point.average, scale.maximum)}
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
          </svg>
          <div
            aria-hidden="true"
            className="pointer-events-none absolute inset-x-0 top-[89%] whitespace-nowrap text-xs tabular-nums text-muted-foreground"
          >
            {timeTicks.map(({ time, label, fraction }, index) => (
              <span
                key={time}
                className={cn("absolute", index % 2 === 1 && "max-sm:hidden")}
                style={{
                  left: `${fraction * 100}%`,
                  transform: `translateX(${fraction < 0.07 ? 0 : fraction > 0.93 ? -100 : -50}%)`,
                }}
              >
                {label}
              </span>
            ))}
          </div>
        </div>
        <p className="sr-only">
          Lines show bucket averages. Shaded bands show recorded minimum and
          maximum values. Missing buckets are gaps, not zero values.
        </p>
      </div>
      <MonitoringBucketDetails
        id={detailsId}
        series={displayedSeries}
        bucket={selected ?? latestBucket}
        label={
          interaction?.pinned
            ? "Pinned bucket"
            : selected
              ? "Selected bucket"
              : "Latest bucket"
        }
        pinned={interaction?.pinned ?? false}
        timeZone={timeZone}
      />
    </div>
  );
}
