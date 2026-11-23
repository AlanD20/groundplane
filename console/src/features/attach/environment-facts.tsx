import { Disclosure } from "@/components/common/disclosure";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
("use client");

import { RevealValue } from "@/components/common/reveal-value";
import { EmptyState } from "@/components/common/empty-state";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useStore } from "@/lib/store";
import type { Attach, Environment, Service } from "@/lib/types";
import { Database } from "lucide-react";

export function FactsCard({
  env,
  service,
}: {
  env: Environment;
  service?: Service;
}) {
  const store = useStore();
  const factGroups = new Map<string, Attach[]>();
  for (const attach of env.attaches) {
    if (service && attach.serviceId !== service.id) continue;
    if (
      !attach.factLoadError &&
      !attach.factSets.some((set) => set.facts.length)
    )
      continue;
    const group = factGroups.get(attach.serviceId);
    if (group) group.push(attach);
    else factGroups.set(attach.serviceId, [attach]);
  }
  const groups = [...factGroups].sort(([, left], [, right]) =>
    left[0].service.localeCompare(right[0].service),
  );

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2">
          <Database className="size-4 text-muted-foreground" /> Connection
          values
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-col gap-3">
        {!groups.length && (
          <EmptyState
            embedded
            icon={<Database />}
            title="No connection values yet"
            description="Connect a Service to a backing service to receive its connection values."
          />
        )}

        {!!groups.length && (
          <p className="text-xs text-muted-foreground">
            View the values each backing connection provides. Use Configuration
            to make a value available to a Service.
          </p>
        )}
        {groups.map(([id, attaches]) => (
          <section key={id} className="flex flex-col gap-2">
            {!service && (
              <h3 className="text-sm font-semibold [overflow-wrap:anywhere]">
                {attaches[0].service || id}
              </h3>
            )}
            {attaches.map((attach) => {
              const backing = store.getBackingProject(attach.projectId);
              return (
                <Disclosure
                  summary={
                    <span className="flex flex-wrap gap-2 text-sm font-medium">
                      {attach.name}
                      <span className="text-xs text-muted-foreground">
                        {backing?.name}
                      </span>
                    </span>
                  }
                  key={attach.id}
                  className={workspaceSectionClassName(
                    false,
                    "flex flex-col gap-3",
                  )}
                >
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
                        className="divide-y divide-border border-t border-border [&>div]:py-3"
                      >
                        {set.grantAttachId && (
                          <p className="pt-2 text-xs font-medium text-muted-foreground">
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
                </Disclosure>
              );
            })}
          </section>
        ))}
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
    <div className="flex min-w-0 flex-col gap-2 sm:flex-row sm:items-start">
      <span className="min-w-0 font-mono text-xs text-muted-foreground [overflow-wrap:anywhere] sm:w-40 sm:shrink-0">
        {k}
      </span>
      <RevealValue
        key={`${attachId}/${grantAttachId ?? ""}/${k}`}
        className="flex-1"
        label={k}
        sensitive={secret}
        loadValue={() => store.revealAttachFact(attachId, k, grantAttachId)}
      />
    </div>
  );
}
