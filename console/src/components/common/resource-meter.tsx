import type { ReactNode } from "react";

export function ResourceMeter({
  icon,
  label,
  pct,
  detail,
}: {
  icon?: ReactNode;
  label: string;
  pct: number;
  detail: string;
}) {
  const tone =
    pct >= 85 ? "bg-destructive" : pct >= 70 ? "bg-warning" : "bg-primary";
  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-start justify-between gap-2 text-xs">
        <span className="flex items-center gap-1.5 text-muted-foreground [&_svg]:size-3.5">
          {icon}
          {label}
        </span>
        <span className="text-right tabular-nums">{detail}</span>
      </div>
      <div
        role="meter"
        aria-label={label}
        aria-valuenow={pct}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuetext={`${pct}%: ${detail}`}
        className="h-1.5 overflow-hidden rounded-full bg-secondary"
      >
        <div
          className={`h-full rounded-full ${tone}`}
          style={{ width: `${Math.max(0, Math.min(100, pct))}%` }}
        />
      </div>
    </div>
  );
}
