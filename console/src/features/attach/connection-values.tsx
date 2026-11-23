import { Link } from "react-router-dom";
import { FactRow } from "./fact-row";
import { Badge } from "@/components/ui/badge";
import type { Attach, Environment } from "@/lib/types";

export function ConnectionValues({
  attach,
  env,
}: {
  attach: Attach;
  env: Environment;
}) {
  const service = env.services.find(
    (item) => item.id === attach.serviceId || item.name === attach.service,
  );
  return (
    <section
      aria-label="Connection values and usage"
      className="space-y-3 border-t border-border pt-3"
    >
      <h3 className="text-sm font-semibold">Connection values &amp; usage</h3>
      <div className="space-y-4">
        {!attach.factSets.some((set) => set.facts.length) && (
          <p className="text-sm text-muted-foreground">
            Values will appear when this connection is ready.
          </p>
        )}
        {attach.factSets.map((set) => (
          <div
            key={set.grantAttachId ?? "own"}
            className="divide-y divide-border"
          >
            {set.grantAttachId && (
              <p className="text-sm text-muted-foreground">
                Shared credentials ·{" "}
                {env.attaches.find(
                  (candidate) => candidate.id === set.grantAttachId,
                )?.name ?? set.grantAttachId}
              </p>
            )}
            {set.facts.map((fact) => {
              const usedBy = env.entries.filter(
                (entry) =>
                  entry.source.kind === "fact" &&
                  entry.source.attachId === attach.id &&
                  (entry.source.grantAttachId ?? "") ===
                    (set.grantAttachId ?? "") &&
                  entry.source.fact === fact.key,
              );
              const query = new URLSearchParams({
                view: "overview",
                service: service?.id ?? "",
                serviceTab: "entries",
                fromConnection: attach.id,
                connectionValue: fact.key,
              });
              if (set.grantAttachId)
                query.set("connectionGrant", set.grantAttachId);
              return (
                <div key={fact.key} className="space-y-2 py-2.5">
                  <FactRow
                    attachId={attach.id}
                    grantAttachId={set.grantAttachId}
                    k={fact.key}
                    secret={fact.secret}
                  />
                  <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                    {usedBy.length ? (
                      <>
                        Used by{" "}
                        {usedBy.map((entry) => (
                          <Badge key={entry.id} variant="muted">
                            {entry.key ?? entry.path}
                          </Badge>
                        ))}
                      </>
                    ) : (
                      <span>Not used in variables or files</span>
                    )}
                    {service && (
                      <Link
                        className="ml-auto text-primary underline underline-offset-4"
                        to={`?${query}`}
                      >
                        Use in variable
                      </Link>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        ))}
      </div>
    </section>
  );
}
