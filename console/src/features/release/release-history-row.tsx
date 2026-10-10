import { Disclosure } from "@/components/common/disclosure";
import { ImageReference } from "@/components/common/image-reference";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { Badge } from "@/components/ui/badge";
import { formatTimestamp } from "@/lib/format-timestamp";
import type { DeployRecord, Environment } from "@/lib/types";
import { ReleaseDetails } from "./release-detail-page";

export function ReleaseHistoryRow({
  release,
  env,
}: {
  release: DeployRecord;
  env: Environment;
}) {
  return (
    <Disclosure
      className={workspaceSectionClassName(false)}
      summaryClassName="gap-3"
      summary={
        <span className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-3">
          <span className="min-w-0 space-y-1">
            <span className="block break-all text-sm font-medium">
              {release.service}
            </span>
            <span className="block text-xs">
              <ImageReference value={release.tag} />
            </span>
            <span className="block text-xs text-muted-foreground">
              {formatTimestamp(release.when, "Time unavailable")} |{" "}
              {release.strategy}
            </span>
          </span>
          <Badge variant={release.status === "active" ? "success" : "outline"}>
            {release.status}
          </Badge>
        </span>
      }
    >
      <div className="mt-4 min-w-0 border-t border-border pt-4">
        <ReleaseDetails id={release.id} environment={env} inline />
      </div>
    </Disclosure>
  );
}
