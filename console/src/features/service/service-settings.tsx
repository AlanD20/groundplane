import { workspaceSectionClassName } from "@/components/common/workspace-section";
import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { useRef, useState, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
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
  const [saved, setSaved] = useState<ServiceFormSection | null>(null);
  const [editing, setEditing] = useState<ServiceFormSection | null>(null);
  const editButtons = useRef<
    Partial<Record<ServiceFormSection, HTMLButtonElement | null>>
  >({});
  const rows: {
    section: ServiceFormSection;
    label: string;
    description: string;
    value: ReactNode;
  }[] = [
    {
      section: "workload",
      label: "Container image",
      description: "Choose the image used by this workload.",
      value: <ImageReference value={service.image} />,
    },
    {
      section: "runtime",
      label: "Runtime & resources",
      description: "Replicas, resource limits and restart behavior.",
      value: (
        <dl className="grid grid-cols-2 gap-x-6 gap-y-4">
          {[
            ["Replicas", String(service.replicas)],
            ["Memory limit", service.resources.mem || "No limit"],
            [
              "CPU limit",
              Number(service.resources.cpus) > 0
                ? `${service.resources.cpus} cores`
                : "No limit",
            ],
            ["Restart", service.restart],
            ["Deployment strategy", service.strategy],
            ["Failure policy", service.onFailure],
          ].map(([label, value]) => (
            <div key={label}>
              <dt className="text-xs text-muted-foreground">{label}</dt>
              <dd className="mt-1 text-sm">{value}</dd>
            </div>
          ))}
        </dl>
      ),
    },
    {
      section: "healthcheck",
      label: "Healthcheck",
      description: "How Groundplane checks readiness.",
      value: service.healthcheck
        ? `${service.healthcheck.kind} · ${service.healthcheck.target}`
        : "Not configured",
    },
    {
      section: "network",
      label: "Networks & ports",
      description: "Zone membership and ports inside the container network.",
      value: (
        <div className="space-y-4">
          <div>
            <p className="mb-2 text-xs text-muted-foreground">Network Zones</p>
            <div className="flex flex-wrap gap-2">
              {service.zones.length ? (
                service.zones.map((id) => (
                  <Badge
                    key={id}
                    variant="muted"
                    className="max-w-full whitespace-normal break-all"
                  >
                    {env.zones.find((zone) => zone.id === id)?.name ?? id}
                  </Badge>
                ))
              ) : (
                <span className="text-sm">No Zones</span>
              )}
            </div>
          </div>
          <div>
            <p className="mb-2 text-xs text-muted-foreground">Exposed ports</p>
            <p className="font-mono text-sm">
              {service.expose.join(", ") || "No exposed ports"}
            </p>
          </div>
        </div>
      ),
    },
    {
      section: "hooks",
      label: "Lifecycle hooks",
      description: "Provisioning and runtime commands.",
      value: "Custom backing lifecycle commands",
    },
  ];
  return (
    <ResourcePanel
      title={
        sections?.length === 1 && sections[0] === "network"
          ? "Networking"
          : "Runtime & image"
      }
    >
      <p className="text-sm text-muted-foreground">
        Saved configuration is applied on the next deploy. Editing here does not
        change running containers.
      </p>
      <div className="space-y-2">
        {rows
          .filter(
            (row) =>
              (!sections || sections.includes(row.section)) &&
              (row.section !== "hooks" || service.adapter === "custom"),
          )
          .map((row) => (
            <section
              key={row.section}
              aria-label={row.label}
              className={workspaceSectionClassName(
                editing === row.section,
                "grid gap-4 xl:grid-cols-[12rem_minmax(0,1fr)] xl:gap-8",
              )}
            >
              <div className="space-y-2">
                <h3 className="text-sm font-semibold">{row.label}</h3>
                <p className="text-xs leading-relaxed text-muted-foreground">
                  {row.description}
                </p>
                {editing === row.section ? (
                  <Badge variant="warning">Editing · Unsaved</Badge>
                ) : saved === row.section ? (
                  <Badge variant="success">Saved</Badge>
                ) : null}
              </div>
              <div className="min-w-0 space-y-4 border-t border-border pt-4 xl:border-l xl:border-t-0 xl:pl-6 xl:pt-0">
                <InlineEditorRegion editing={editing === row.section}>
                  {editing === row.section ? (
                    <ServiceFormBody
                      inline
                      key={`${service.id}/${editing}`}
                      env={env}
                      workspace={workspace}
                      initial={service}
                      section={editing}
                      onSaved={() => {
                        setSaved(row.section);
                        onSaved?.();
                      }}
                      onClose={() => {
                        setEditing(null);
                        requestAnimationFrame(() =>
                          editButtons.current[row.section]?.focus({
                            preventScroll: true,
                          }),
                        );
                      }}
                    />
                  ) : (
                    <div className="flex min-w-0 items-start justify-between gap-4">
                      <div className="min-w-0 flex-1 text-sm [overflow-wrap:anywhere]">
                        {row.value}
                      </div>
                      {row.section === "workload" && imageAction ? (
                        imageAction
                      ) : (
                        <Button
                          variant="outline"
                          size="sm"
                          aria-label={`Edit ${row.label.toLowerCase()}`}
                          disabled={editing !== null}
                          ref={(node) => {
                            editButtons.current[row.section] = node;
                          }}
                          onClick={() => {
                            setSaved(null);
                            setEditing(row.section);
                          }}
                        >
                          Edit
                        </Button>
                      )}
                    </div>
                  )}
                </InlineEditorRegion>
                {saved === row.section && editing !== row.section && (
                  <p role="status" className="text-xs text-success">
                    Saved configuration. Deploy to apply changes.
                  </p>
                )}
                {row.section === "workload" && service.command && (
                  <p className="break-all text-xs text-muted-foreground">
                    Command: <code>{service.command}</code> · managed in
                    Blueprint
                  </p>
                )}
                {row.section === "network" &&
                  (service.aliases.length > 0 ||
                    service.dependsOn.length > 0) && (
                    <div className="space-y-1 border-t border-border pt-3 text-xs text-muted-foreground [overflow-wrap:anywhere]">
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
              </div>
            </section>
          ))}
      </div>
    </ResourcePanel>
  );
}
