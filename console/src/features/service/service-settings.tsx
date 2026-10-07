import { useState, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { ServiceFormBody } from "@/components/common/service-form-body";
import type { ServiceFormSection } from "@/components/common/service-form-types";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ImageReference } from "@/components/common/image-reference";
import type { Environment, Service } from "@/lib/types";

export function ServiceSettings({
  env,
  service,
  workspace,
  imageAction,
  onSaved,
  sections,
}: {
  env: Environment;
  service: Service;
  workspace: string;
  imageAction?: ReactNode;
  onSaved?: () => void;
  sections?: ServiceFormSection[];
}) {
  const [saved, setSaved] = useState(false);
  const [editing, setEditing] = useState<ServiceFormSection | null>(null);
  const rows: {
    section: ServiceFormSection;
    label: string;
    value: ReactNode;
  }[] = [
    {
      section: "workload",
      label: "Container image",
      value: <ImageReference value={service.image} />,
    },
    {
      section: "runtime",
      label: "Runtime & resources",
      value: `${service.replicas} ${service.replicas === 1 ? "replica" : "replicas"} · ${service.resources.mem || "No memory limit"} · ${Number(service.resources.cpus) > 0 ? `${service.resources.cpus} CPU` : "No CPU limit"} · ${service.strategy}`,
    },
    {
      section: "healthcheck",
      label: "Healthcheck",
      value: service.healthcheck
        ? `${service.healthcheck.kind} · ${service.healthcheck.target}`
        : "Not configured",
    },
    {
      section: "network",
      label: "Networking",
      value: `${service.zones.map((id) => env.zones.find((zone) => zone.id === id)?.name ?? id).join(", ") || "No Zones"} · ${service.expose.join(", ") || "No exposed ports"}`,
    },
  ];
  return (
    <>
      <ResourcePanel
        title={
          sections?.length === 1 && sections[0] === "network"
            ? "Networks & ports"
            : "Runtime & image"
        }
      >
        <p className="text-sm text-muted-foreground">
          Saving updates desired configuration, not running containers.{" "}
          {workspace !== "platform" && "Deploy to apply these changes."}
        </p>
        <div className="divide-y divide-border">
          {rows
            .filter((row) => !sections || sections.includes(row.section))
            .map((row) => (
              <div
                key={row.section}
                className="space-y-4 py-5 first:pt-0 last:pb-0"
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0 space-y-1">
                    <h3 className="text-sm font-medium">{row.label}</h3>
                    <div className="[overflow-wrap:anywhere] text-sm text-muted-foreground">
                      {row.value}
                    </div>
                  </div>
                  {row.section === "workload" && imageAction ? (
                    imageAction
                  ) : (
                    <Button
                      variant="outline"
                      size="sm"
                      aria-label={`Edit ${row.label.toLowerCase()}`}
                      disabled={editing !== null}
                      onClick={() => {
                        setSaved(false);
                        setEditing(row.section);
                      }}
                    >
                      Edit
                    </Button>
                  )}
                </div>
                {row.section === "workload" && service.command && (
                  <p className="break-all text-sm text-muted-foreground">
                    <span className="font-medium">Command: </span>
                    <code>{service.command}</code> · managed in Blueprint
                  </p>
                )}
                {row.section === "network" &&
                  (service.aliases.length > 0 ||
                    service.dependsOn.length > 0) && (
                    <div className="space-y-1 text-sm text-muted-foreground">
                      <p>Aliases: {service.aliases.join(", ") || "None"}</p>
                      <p>
                        Dependencies: {service.dependsOn.join(", ") || "None"}
                      </p>
                      <p>
                        Update aliases and dependencies in the Environment
                        Blueprint.
                      </p>
                    </div>
                  )}
                {editing === row.section && (
                  <ServiceFormBody
                    inline
                    key={`${service.id}/${editing}`}
                    env={env}
                    workspace={workspace}
                    initial={service}
                    section={editing}
                    onSaved={() => {
                      setSaved(true);
                      onSaved?.();
                    }}
                    onClose={() => setEditing(null)}
                  />
                )}
              </div>
            ))}
          {service.adapter === "custom" &&
            (!sections || sections.includes("hooks")) && (
              <div className="flex items-center justify-between gap-4 py-5">
                <div>
                  <h3 className="text-sm font-medium">Lifecycle hooks</h3>
                  <p className="text-sm text-muted-foreground">
                    Provisioning and runtime commands
                  </p>
                </div>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={editing !== null}
                  onClick={() => {
                    setSaved(false);
                    setEditing("hooks");
                  }}
                >
                  Edit hooks
                </Button>
              </div>
            )}
        </div>
      </ResourcePanel>
      {editing === "hooks" && (
        <ServiceFormBody
          inline
          env={env}
          workspace={workspace}
          initial={service}
          section="hooks"
          onSaved={() => {
            setSaved(true);
            onSaved?.();
          }}
          onClose={() => setEditing(null)}
        />
      )}
      {saved && <p role="status">Saved. Deploy to apply changes.</p>}
    </>
  );
}
