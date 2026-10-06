import { useState, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Drawer } from "@/components/ui/drawer";
import {
  ServiceFormBody,
  type ServiceFormSection,
} from "@/components/common/service-form-body";
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
      value: `${service.zones.join(", ") || "No Zones"} · ${service.expose.join(", ") || "No exposed ports"}`,
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
                className="flex items-center justify-between gap-4 py-5 first:pt-0 last:pb-0"
              >
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
                    onClick={() => setEditing(row.section)}
                  >
                    Edit
                  </Button>
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
                  onClick={() => setEditing("hooks")}
                >
                  Edit hooks
                </Button>
              </div>
            )}
        </div>
      </ResourcePanel>
      <Drawer
        open={editing !== null}
        onOpenChange={(open) => {
          if (!open) setEditing(null);
        }}
      >
        {editing && (
          <ServiceFormBody
            key={`${service.id}/${editing}`}
            env={env}
            workspace={workspace}
            initial={service}
            section={editing}
            onClose={() => {
              setEditing(null);
              onSaved?.();
            }}
          />
        )}
      </Drawer>
    </>
  );
}
