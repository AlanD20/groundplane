import { Disclosure } from "@/components/common/disclosure";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
("use client");

import { RevealValue } from "@/components/common/reveal-value";
import { EmptyState } from "@/components/common/empty-state";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useStore } from "@/lib/store";
import type { Environment, Service } from "@/lib/types";
import { Database } from "lucide-react";

export function FactsCard({
  env,
  service,
}: {
  env: Environment;
  service?: Service;
}) {
  const store = useStore();
  const factGroups = env.attaches
    .filter(
      (attach) =>
        !service ||
        attach.serviceId === service.id ||
        attach.service === service.name,
    )
    .flatMap((a) => {
      const g = store.getBackingProject(a.projectId);
      if (!g) return [];
      const sets = a.factSets.map((set) => ({
        label: set.grantAttachId
          ? `${g.name} · grant ${set.grantAttachId}`
          : `${g.name} · ${a.name}`,
        rows: set.facts.map((fact) => ({
          attachId: a.id,
          grantAttachId: set.grantAttachId,
          k: fact.key,
          secret: fact.secret,
        })),
      }));
      return [{ id: `facts-${a.id}`, title: `for ${a.service}`, sets }];
    });

  if (factGroups.length === 0)
    return (
      <EmptyState
        icon={<Database />}
        title="No connection values yet"
        description="Connect a Service to a backing service. After provisioning, map its connection values to variables or files explicitly."
      />
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
        <p className="text-xs text-muted-foreground">
          Connection outputs. Manage access in Backing connections; map values
          in Variables & files.
        </p>
        {factGroups.map((grp) => (
          <div key={grp.id} className="flex flex-col gap-2">
            <span className="font-mono text-xs text-muted-foreground">
              {grp.title}
            </span>
            {grp.sets.map((set) => (
              <Disclosure
                summary={
                  <span className="flex flex-wrap gap-2 text-sm font-medium">
                    {set.label}
                    <span className="text-xs text-muted-foreground">
                      {set.rows.length} values
                    </span>
                  </span>
                }
                key={set.label}
                className={workspaceSectionClassName(
                  false,
                  "flex flex-col gap-3",
                )}
              >
                <div className="flex flex-col divide-y divide-border border-t border-border [&>div]:py-3">
                  {set.rows.map((r) => (
                    <FactRow key={r.k} {...r} />
                  ))}
                </div>
              </Disclosure>
            ))}
          </div>
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
