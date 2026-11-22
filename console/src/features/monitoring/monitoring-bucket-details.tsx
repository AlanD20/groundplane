import {
  pointCenter,
  pointForTime,
  type MonitoringChartBucketPoint,
  type MonitoringChartSeries,
} from "./monitoring-chart-geometry";

const valueFormatter = new Intl.NumberFormat(undefined, {
  maximumSignificantDigits: 4,
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

function formatValue(value: number | null, unit: string): string {
  if (value === null || !Number.isFinite(value)) return "Unavailable";
  return `${valueFormatter.format(value)} ${unit}`;
}

function formatCoverage(valid: number, expected: number): string {
  if (expected === 0) return "Not applicable";
  return `${valid.toLocaleString()} / ${expected.toLocaleString()}`;
}

export function MonitoringBucketDetails({
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
