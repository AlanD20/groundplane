import { Button } from "@/components/ui/button";
import type { Environment } from "@/lib/types";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useSearchParams } from "react-router-dom";
import { ServiceCards } from "./service-cards";
import { ServicesList } from "./services-list";

export function EnvironmentOverview({
  env,
  now,
}: {
  env: Environment;
  now: number;
}) {
  const [search, setSearch] = useSearchParams();
  const listView = search.get("layout") === "list";
  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <h2 className="text-lg font-semibold">Services</h2>
          <p className="text-xs text-muted-foreground">
            Workloads in this Environment. Open a Service to manage its runtime,
            storage and connections.
          </p>
        </div>
        <div className="flex gap-1" role="group" aria-label="Service display">
          {(["cards", "list"] as const).map((layout) => (
            <Button
              key={layout}
              size="sm"
              variant={listView === (layout === "list") ? "secondary" : "ghost"}
              aria-pressed={listView === (layout === "list")}
              onClick={() =>
                setSearch((current) => {
                  const next = new URLSearchParams(current);
                  next.set("layout", layout);
                  return next;
                })
              }
            >
              {layout === "cards" ? "Cards" : "Table"}
            </Button>
          ))}
        </div>
      </div>
      {listView ? (
        <ServicesList env={env} now={now} />
      ) : (
        <ServiceCards env={env} now={now} />
      )}
      <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border pt-4 text-xs text-muted-foreground">
        <span>
          Last deployment: {formatTimestamp(env.lastDeployAt, "Never")}
        </span>
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setSearch({ view: "operations", panel: "releases" })}
        >
          Deployment history
        </Button>
      </div>
    </div>
  );
}
