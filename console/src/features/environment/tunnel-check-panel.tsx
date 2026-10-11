import { useState } from "react";
import { PlugZap } from "lucide-react";
import { Disclosure } from "@/components/common/disclosure";
import { FormSection } from "@/components/ui/form-section";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import type { Environment } from "@/lib/types";
import { EdgeCheckPanel } from "./edge-check-panel";

export function TunnelAccessCheck({
  env,
  connectorId,
}: {
  env: Environment;
  connectorId: string;
}) {
  const [open, setOpen] = useState(false);
  const [originServiceId, setOriginServiceId] = useState("");
  const [port, setPort] = useState("");
  const [hostname, setHostname] = useState("");
  const [path, setPath] = useState("/");
  const generated = new Set(
    env.components.flatMap((component) => component.generatedServices),
  );
  const services = env.services.filter((service) => !generated.has(service.id));
  const number = Number(port);
  return (
    <Disclosure
      open={open}
      onOpenChange={setOpen}
      summary={
        <span className="flex items-center gap-2 text-xs font-medium">
          <PlugZap className="size-3.5" />
          Test Tunnel access
        </span>
      }
    >
      <div className="space-y-3 pt-3">
        <FormSection
          title="Origin and public address"
          description="Use the hostname configured in Cloudflare. GP tests it without reading or changing provider ingress."
        >
          <div className="space-y-1.5">
            <Label htmlFor={`tunnel-origin-${connectorId}`}>
              Origin Service
            </Label>
            <Select
              id={`tunnel-origin-${connectorId}`}
              searchable
              value={originServiceId}
              onValueChange={setOriginServiceId}
              placeholder="Select a deployed Service"
              options={services.map((service) => ({
                value: service.id,
                label: service.name,
              }))}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor={`tunnel-port-${connectorId}`}>
              Origin HTTP port
            </Label>
            <Input
              id={`tunnel-port-${connectorId}`}
              type="number"
              min={1}
              max={65535}
              value={port}
              onChange={(event) => setPort(event.target.value)}
              placeholder="80"
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor={`tunnel-hostname-${connectorId}`}>
              Public hostname
            </Label>
            <Input
              id={`tunnel-hostname-${connectorId}`}
              value={hostname}
              onChange={(event) =>
                setHostname(event.target.value.trim().toLowerCase())
              }
              placeholder="app.example.com"
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor={`tunnel-path-${connectorId}`}>Request path</Label>
            <Input
              id={`tunnel-path-${connectorId}`}
              value={path}
              onChange={(event) => setPath(event.target.value)}
              placeholder="/health"
            />
          </div>
        </FormSection>
        <EdgeCheckPanel
          environmentId={env.id}
          connectorId={connectorId}
          tunnel={{ originServiceId, port: number, hostname, path }}
          open={open}
          disabled={
            !services.some((service) => service.id === originServiceId) ||
            !Number.isInteger(number) ||
            number < 1 ||
            number > 65535 ||
            !hostname ||
            !path.startsWith("/")
          }
        />
      </div>
    </Disclosure>
  );
}
