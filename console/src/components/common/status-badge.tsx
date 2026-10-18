import { cn } from "@/lib/utils";
import type { HealthState } from "@/lib/types";

const MAP: Record<string, { label: string; dot: string; text: string }> = {
  succeeded: { label: "Succeeded", dot: "bg-success", text: "text-success" },
  queued: { label: "Queued", dot: "bg-warning", text: "text-warning" },
  preparing: { label: "Preparing", dot: "bg-info", text: "text-info" },
  waiting: { label: "Waiting", dot: "bg-warning", text: "text-warning" },
  assigned: { label: "Assigned", dot: "bg-info", text: "text-info" },
  ready: { label: "Ready", dot: "bg-info", text: "text-info" },
  attention: {
    label: "Needs attention",
    dot: "bg-warning",
    text: "text-warning",
  },
  attention_required: {
    label: "Needs attention",
    dot: "bg-warning",
    text: "text-warning",
  },
  stopping: { label: "Stopping", dot: "bg-warning", text: "text-warning" },
  finishing: { label: "Finishing", dot: "bg-info", text: "text-info" },
  cancelled: {
    label: "Cancelled",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
  superseded: {
    label: "Superseded",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
  healthy: { label: "Healthy", dot: "bg-success", text: "text-success" },
  online: { label: "Online", dot: "bg-success", text: "text-success" },
  completed: { label: "Completed", dot: "bg-success", text: "text-success" },
  running: {
    label: "Running",
    dot: "bg-info animate-pulse",
    text: "text-info",
  },
  starting: {
    label: "Starting",
    dot: "bg-warning animate-pulse",
    text: "text-warning",
  },
  degraded: { label: "Degraded", dot: "bg-warning", text: "text-warning" },
  pending: { label: "Pending", dot: "bg-warning", text: "text-warning" },
  failed: { label: "Failed", dot: "bg-destructive", text: "text-destructive" },
  timed_out: {
    label: "Timed out",
    dot: "bg-destructive",
    text: "text-destructive",
  },
  stopped: {
    label: "Stopped",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
  unknown: {
    label: "Unknown",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
  absent: {
    label: "Absent",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
  unavailable: {
    label: "Unavailable",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
  offline: {
    label: "Offline",
    dot: "bg-muted-foreground",
    text: "text-muted-foreground",
  },
};

export function StatusDot({
  status,
  className,
}: {
  status: HealthState | string;
  className?: string;
}) {
  const m = MAP[status] ?? MAP.unknown;
  return (
    <span
      className={cn(
        "inline-block size-2 shrink-0 rounded-full",
        m.dot,
        className,
      )}
      aria-hidden
    />
  );
}

export function StatusBadge({
  status,
  label,
  className,
}: {
  status: HealthState | string;
  label?: string;
  className?: string;
}) {
  const m = MAP[status] ?? MAP.unknown;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1.5 whitespace-nowrap text-xs font-medium",
        m.text,
        className,
      )}
    >
      <span className={cn("size-1.5 rounded-full", m.dot)} aria-hidden />
      {label ?? m.label}
    </span>
  );
}
