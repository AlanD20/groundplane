import { cn } from "@/lib/utils";

export function StatCard({
  label,
  value,
  valueTitle,
  hint,
  icon,
  tone = "default",
  className,
}: {
  label: string;
  value: React.ReactNode;
  valueTitle?: string;
  hint?: React.ReactNode;
  icon?: React.ReactNode;
  tone?: "default" | "success" | "warning" | "danger";
  className?: string;
}) {
  const toneText =
    tone === "success"
      ? "text-success"
      : tone === "warning"
        ? "text-warning"
        : tone === "danger"
          ? "text-destructive"
          : "text-foreground";
  return (
    <div
      className={cn(
        "flex min-w-0 flex-col gap-3 rounded-xl border border-border bg-card p-5",
        className,
      )}
    >
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-muted-foreground">
          {label}
        </span>
        {icon && (
          <span className="text-muted-foreground [&_svg]:size-4">{icon}</span>
        )}
      </div>
      <div
        title={valueTitle ?? (typeof value === "string" ? value : undefined)}
        className={cn(
          "break-words text-xl font-medium tracking-tight tabular-nums leading-tight",
          toneText,
        )}
      >
        {value}
      </div>
      {hint && <div className="text-xs text-muted-foreground">{hint}</div>}
    </div>
  );
}
