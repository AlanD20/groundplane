import { Link, useLocation } from "react-router-dom";
import { ArrowUpRight } from "lucide-react";
import { cn } from "@/lib/utils";

export function TaskLink({
  taskId,
  onClick,
  children = "Open task",
  className,
  label,
}: {
  taskId: string;
  onClick?: () => void;
  children?: React.ReactNode;
  className?: string;
  label?: string;
}) {
  const location = useLocation();
  const search = new URLSearchParams(location.search);
  search.set("task", taskId);
  return (
    <Link
      to={{
        pathname: location.pathname,
        search: `?${search}`,
        hash: location.hash,
      }}
      onClick={onClick}
      aria-label={label ?? `Open task ${taskId}`}
      className={cn(
        "inline-flex max-w-full items-center gap-1 rounded-sm text-xs text-primary underline underline-offset-4 focus-visible:outline-2 focus-visible:outline-ring",
        className,
      )}
    >
      <span className="min-w-0 break-all">{children}</span>
      <ArrowUpRight className="size-3.5 shrink-0" />
    </Link>
  );
}
