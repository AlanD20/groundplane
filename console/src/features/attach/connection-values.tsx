import { Link } from "react-router-dom";
import { FactRow } from "./environment-facts";
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
    <details className="group border-t border-border pt-3">
      <summary className="cursor-pointer text-sm font-medium">
        Connection values & usage
      </summary>
      <div className="mt-4 space-y-5">
        {!attach.factSets.some((set) => set.facts.length) && (
          <p className="text-sm text-muted-foreground">
            Values will appear when this connection is ready.
          </p>
        )}
        {attach.factSets.map((set) => (
          <section key={set.grantAttachId ?? "own"} className="space-y-4">
            {set.grantAttachId && (
              <p className="text-sm text-muted-foreground">
                Shared database · {set.grantAttachId}
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
                <div
                  key={fact.key}
                  className="space-y-2 rounded-lg bg-muted/20 p-3"
                >
                  <FactRow
                    attachId={attach.id}
                    grantAttachId={set.grantAttachId}
                    k={fact.key}
                    secret={fact.secret}
                  />
                  <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
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
          </section>
        ))}
      </div>
    </details>
  );
}
