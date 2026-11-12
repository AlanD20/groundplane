import { cn } from "@/lib/utils";
import { AlertTriangle, Inbox, LoaderCircle } from "lucide-react";

export function EmptyState({
  icon,
  title,
  description,
  action,
  className,
  embedded = false,
  loading = false,
  error = false,
}: {
  icon?: React.ReactNode;
  title: string;
  description?: string;
  action?: React.ReactNode;
  className?: string;
  embedded?: boolean;
  loading?: boolean;
  error?: boolean;
}) {
  return (
    <div
      role={error ? "alert" : "status"}
      className={cn(
        "flex flex-col items-center justify-center gap-3 px-6 py-12 text-center font-sans",
        !embedded && "rounded-lg border border-border bg-card",
        className,
      )}
    >
      <div
        className={cn(
          "flex size-10 items-center justify-center rounded-lg border border-border bg-surface text-muted-foreground [&_svg]:size-5",
          error && "text-destructive",
        )}
      >
        {loading ? (
          <LoaderCircle className="animate-spin motion-reduce:animate-none" />
        ) : error ? (
          <AlertTriangle />
        ) : (
          (icon ?? <Inbox />)
        )}
      </div>
      <div className="flex flex-col gap-1">
        <p className="text-sm font-medium">{title}</p>
        {description && (
          <p className="max-w-sm text-sm text-muted-foreground">
            {description}
          </p>
        )}
      </div>
      {action}
    </div>
  );
}
