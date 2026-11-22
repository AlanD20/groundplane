export type MonitoringChartBucketPoint = {
  /** Inclusive UTC Unix timestamp in milliseconds. */
  readonly from: number;
  /** Exclusive UTC Unix timestamp in milliseconds. */
  readonly to: number;
  readonly average: number | null;
  readonly minimum: number | null;
  readonly maximum: number | null;
  readonly validSamples: number;
  readonly expectedSamples: number;
  readonly validContributions: number;
  readonly expectedContributions: number;
};

export type MonitoringChartSeries = {
  readonly id: string;
  readonly label: string;
  readonly unit: string;
  readonly color: string;
  readonly points: readonly MonitoringChartBucketPoint[];
};

export type MonitoringChartWindow = {
  readonly from: number;
  readonly to: number;
};

export const chartPlot = {
  left: 62,
  right: 786,
  top: 38,
  bottom: 210,
} as const;

function finiteValue(value: number | null): value is number {
  return value !== null && Number.isFinite(value) && value >= 0;
}

export function chartDomainMaximum(
  series: readonly MonitoringChartSeries[],
): number {
  let maximum = 0;
  for (const item of series) {
    for (const point of item.points) {
      for (const value of [point.average, point.minimum, point.maximum]) {
        if (finiteValue(value)) maximum = Math.max(maximum, value);
      }
    }
  }
  return maximum > 0 ? maximum * 1.08 : 1;
}

export function chartX(time: number, window: MonitoringChartWindow): number {
  const span = Math.max(1, window.to - window.from);
  const fraction = Math.min(1, Math.max(0, (time - window.from) / span));
  return chartPlot.left + fraction * (chartPlot.right - chartPlot.left);
}

export function chartY(value: number, maximum: number): number {
  const fraction = Math.min(1, Math.max(0, value / Math.max(maximum, 1e-9)));
  return chartPlot.bottom - fraction * (chartPlot.bottom - chartPlot.top);
}

function pointCenter(point: MonitoringChartBucketPoint): number {
  return point.from + (point.to - point.from) / 2;
}

export function averagePath(
  points: readonly MonitoringChartBucketPoint[],
  window: MonitoringChartWindow,
  maximum: number,
): string {
  let path = "";
  let continuing = false;
  let previousTo = 0;
  for (const point of points) {
    if (!finiteValue(point.average)) {
      continuing = false;
      continue;
    }
    const command = continuing && point.from <= previousTo ? "L" : "M";
    path += `${command}${chartX(pointCenter(point), window).toFixed(2)},${chartY(point.average, maximum).toFixed(2)} `;
    continuing = true;
    previousTo = point.to;
  }
  return path.trim();
}

export function rangeBandPath(
  points: readonly MonitoringChartBucketPoint[],
  window: MonitoringChartWindow,
  maximum: number,
): string {
  const ranges: MonitoringChartBucketPoint[][] = [];
  let range: MonitoringChartBucketPoint[] = [];
  for (const point of points) {
    if (!finiteValue(point.minimum) || !finiteValue(point.maximum)) {
      if (range.length > 0) ranges.push(range);
      range = [];
      continue;
    }
    const previous = range.at(-1);
    if (previous && point.from > previous.to) {
      ranges.push(range);
      range = [];
    }
    range.push(point);
  }
  if (range.length > 0) ranges.push(range);
  return ranges
    .map((values) => {
      const upper = values
        .map((point, index) => {
          const command = index === 0 ? "M" : "L";
          return `${command}${chartX(pointCenter(point), window).toFixed(2)},${chartY(point.maximum!, maximum).toFixed(2)}`;
        })
        .join(" ");
      const lower = [...values]
        .reverse()
        .map(
          (point) =>
            `L${chartX(pointCenter(point), window).toFixed(2)},${chartY(point.minimum!, maximum).toFixed(2)}`,
        )
        .join(" ");
      return `${upper} ${lower} Z`;
    })
    .join(" ");
}

export function bucketIndexAtTime(
  points: readonly MonitoringChartBucketPoint[],
  time: number,
): number | null {
  if (points.length === 0) return null;
  let low = 0;
  let high = points.length - 1;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const point = points[middle];
    if (time < point.from) high = middle - 1;
    else if (time >= point.to) low = middle + 1;
    else return middle;
  }
  if (low <= 0) return 0;
  if (low >= points.length) return points.length - 1;
  const previousDistance = Math.abs(time - pointCenter(points[low - 1]));
  const nextDistance = Math.abs(time - pointCenter(points[low]));
  return previousDistance <= nextDistance ? low - 1 : low;
}

export function frozenSeries(
  series: readonly MonitoringChartSeries[],
): readonly MonitoringChartSeries[] {
  return series.map((item) => ({
    ...item,
    points: item.points.map((point) => ({ ...point })),
  }));
}
