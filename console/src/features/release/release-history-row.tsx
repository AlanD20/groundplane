import { ChevronRight } from "lucide-react";
import { Link } from "react-router-dom";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { Badge } from "@/components/ui/badge";
import { formatTimestamp } from "@/lib/format-timestamp";
import type { DeployRecord } from "@/lib/types";

export function ReleaseHistoryRow({
  release,
  to,
}: {
  release: DeployRecord;
  to: string;
}) {
  return (
    <Link
      to={to}
      className={`${workspaceSectionClassName(false)} flex items-center gap-3 transition-colors hover:bg-muted/60 focus-visible:outline-2 focus-visible:outline-ring`}
    >
      <span className="min-w-0 flex-1 space-y-1">
        <span className="block break-words text-sm font-medium">
          {release.tag || release.service}
        </span>
        <span className="block text-xs text-muted-foreground">
          {formatTimestamp(release.when, "Time unavailable")} ·{" "}
          {release.strategy}
        </span>
      </span>
      <Badge
        variant={
          release.status === "active"
            ? "success"
            : ["failed", "timed_out", "recovery_required"].includes(
                  release.state,
                )
              ? "danger"
              : "outline"
        }
      >
        {release.status === "active"
          ? "Serving"
          : release.state.replaceAll("_", " ")}
      </Badge>
      <ChevronRight
        aria-hidden
        className="size-4 shrink-0 text-muted-foreground"
      />
    </Link>
  );
}
