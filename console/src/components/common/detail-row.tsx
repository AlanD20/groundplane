export function DetailRow({
  label,
  value,
  mono,
}: {
  label: string;
  value: ReactNode;
  mono?: boolean;
}) {
  return (
    <div className="flex min-w-0 items-start justify-between gap-4 border-b border-border py-2 text-xs last:border-0">
      <span className="shrink-0 text-muted-foreground">{label}</span>
      <span
        className={
          mono
            ? "min-w-0 max-w-[65%] break-all text-right font-mono"
            : "min-w-0 max-w-[65%] break-words text-right"
        }
      >
        {value || "—"}
      </span>
    </div>
  );
}
import type { ReactNode } from "react";
