import { cn } from "@/lib/utils";
import { MonitoringBucketDetails } from "./monitoring-bucket-details";
import {
  memo,
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
  pointCenter,
  pointForTime,
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

const axisValueFormatter = new Intl.NumberFormat(undefined, {
  maximumSignificantDigits: 4,
});

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

function readableAxis(maximum: number) {
  const interval = maximum / valueTickCount;
  const order = 10 ** Math.floor(Math.log10(interval));
  const fraction = interval / order;
  const nearest = [2, 2.5, 5, 10].reduce(
    (current, value) =>
      Math.abs(value - fraction) < Math.abs(current - fraction)
        ? value
        : current,
    1,
  );
  const step = nearest * order;
  const intervals = Math.ceil(maximum / step);
  return {
    top: intervals * step,
    labels: Array.from(
      { length: intervals + 1 },
      (_, index) => (intervals - index) * step,
    ),
  };
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
    .map((item) => `${item.id}\u0000${item.label}\u0000${item.color}`)
    .join("\u0001")}`;
}

export const MonitoringChart = memo(function MonitoringChart({
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
    const { top, labels } = readableAxis(maximum);
    return {
      maximum: top,
      labels,
      detailPlaces: Math.min(12, Math.max(2, 3 - Math.floor(Math.log10(top)))),
    };
  }, [displayedSeries]);
  const valueTicks = scale.labels.map((value, index) => ({
    value,
    y:
      chartPlot.top +
      (index / (scale.labels.length - 1)) * (chartPlot.bottom - chartPlot.top),
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
      setInteraction((current) => {
        const nextPinned = pinned ?? current?.pinned ?? false;
        if (current?.index === index && current.pinned === nextPinned)
          return current;
        return {
          index,
          pinned: nextPinned,
          series: current?.series ?? frozenSeries(series),
          window: current?.window ?? { ...window },
        };
      });
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

  const rootStyle = { height, minHeight: 280 } satisfies CSSProperties;
  if (!reference || !latestBucket) {
    return (
      <div
        className={cn(
          "rounded-lg border border-dashed border-border bg-surface/40",
          className,
        )}
      >
        <div
          role="status"
          className="grid place-items-center px-4 text-center text-sm text-muted-foreground"
          style={rootStyle}
        >
          No samples are available for this window.
        </div>
      </div>
    );
  }

  const selectedX = selected
    ? chartX(pointCenter(selected), displayedWindow)
    : null;

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
          "relative flex min-w-0 flex-col overflow-hidden",
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
        <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1 px-3 pt-2 text-xs">
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
        <div className="grid min-h-0 flex-1 grid-cols-[max-content_minmax(0,1fr)] gap-2 pl-3 pr-5">
          <div
            aria-hidden="true"
            className="pointer-events-none grid whitespace-nowrap text-right text-xs tabular-nums text-muted-foreground"
          >
            {valueTicks.map(({ value, y }) => (
              <div className="col-start-1 row-start-1" key={y}>
                <span
                  className="relative inline-block -translate-y-1/2"
                  style={{ top: `${(y / viewBoxHeight) * 100}%` }}
                >
                  {axisValueFormatter.format(value)}
                </span>
              </div>
            ))}
          </div>
          <div className="relative min-h-0">
            <svg
              ref={plot}
              className="absolute inset-0 size-full"
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
        decimalPlaces={scale.detailPlaces}
      />
    </div>
  );
});
