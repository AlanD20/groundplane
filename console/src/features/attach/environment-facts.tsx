import { RevealValue } from "@/components/common/reveal-value";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import { Database } from "lucide-react";
import { ConnectionInspection } from "./connection-inspection";

export function FactsCard({
  env,
  service,
}: {
  env: Environment;
  service?: Service;
}) {
  const attaches = env.attaches.filter(
    (attach) =>
      (!service || attach.serviceId === service.id) &&
      (attach.factLoadError || attach.factSets.some((set) => set.facts.length)),
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" /> Connection
          values
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {attaches.length > 0 && (
          <p className="text-xs text-muted-foreground">
            Use Configuration to make a connection value available to a Service.
          </p>
        )}
        <ConnectionInspection
          scope={service?.id ?? env.id}
          attaches={attaches}
          showValues
        >
          {(attach, backingName) => (
            <>
              <header className="border-b border-border pb-3">
                <h3 className="text-sm font-semibold [overflow-wrap:anywhere]">
                  {attach.name}
                </h3>
                <p className="text-xs text-muted-foreground">
                  {[attach.service, backingName].filter(Boolean).join(" · ")}
                </p>
              </header>
              {attach.factLoadError && (
                <p
                  role="alert"
                  className="text-sm text-destructive [overflow-wrap:anywhere]"
                >
                  {attach.factLoadError}
                </p>
              )}
              {attach.factSets
                .filter((set) => set.facts.length > 0)
                .map((set) => (
                  <div
                    key={set.grantAttachId ?? "own"}
                    className="divide-y divide-border [&>div]:py-3"
                  >
                    {set.grantAttachId && (
                      <p className="py-2 text-xs font-medium text-muted-foreground">
                        Granted by{" "}
                        {env.attaches.find(
                          (item) => item.id === set.grantAttachId,
                        )?.name ?? set.grantAttachId}
                      </p>
                    )}
                    {set.facts.map((fact) => (
                      <FactRow
                        key={fact.key}
                        attachId={attach.id}
                        grantAttachId={set.grantAttachId}
                        k={fact.key}
                        secret={fact.secret}
                      />
                    ))}
                  </div>
                ))}
            </>
          )}
        </ConnectionInspection>
      </CardContent>
    </Card>
  );
}

export function FactRow({
  attachId,
  grantAttachId,
  k,
  secret,
}: {
  attachId: string;
  grantAttachId?: string;
  k: string;
  secret: boolean;
}) {
  const store = useStore();
  return (
    <div className="grid min-w-0 grid-cols-[minmax(0,min(10rem,45%))_minmax(0,1fr)] items-center gap-2">
      <span className="min-w-0 font-mono text-xs text-muted-foreground [overflow-wrap:anywhere]">
        {k}
      </span>
      <RevealValue
        key={`${attachId}/${grantAttachId ?? ""}/${k}`}
        label={k}
        sensitive={secret}
        loadValue={() => store.revealAttachFact(attachId, k, grantAttachId)}
      />
    </div>
  );
}
