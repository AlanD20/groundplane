import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import type { Service } from "@/lib/types";

const tunnelGrant = "component:edge-tunnel";

export function EntryExposure({
  services, value, allowTunnel, onChange, onTunnelSelect,
}: {
  services: Service[];
  value: string[];
  allowTunnel: boolean;
  onChange: (value: string[]) => void;
  onTunnelSelect: () => void;
}) {
  function toggleService(name: string) {
    const selected = value.filter((candidate) => candidate !== "all" && candidate !== tunnelGrant);
    onChange(selected.includes(name)
      ? selected.filter((candidate) => candidate !== name)
      : [...selected, name]);
  }

  return (
    <div className="flex flex-col gap-1.5 md:col-span-2">
      <Label>Available to</Label>
      <div className="flex flex-wrap gap-2">
        <Button
          variant={value.includes("all") ? "default" : "outline"}
          size="sm"
          onClick={() => onChange(["all"])}
        >
          All services
        </Button>
        {allowTunnel && (
          <Button
            variant={value.includes(tunnelGrant) ? "default" : "outline"}
            size="sm"
            onClick={() => {
              onChange([tunnelGrant]);
              onTunnelSelect();
            }}
          >
            Cloudflare Tunnel only
          </Button>
        )}
        {services.map((service) => (
          <Button
            key={service.id}
            variant={value.includes(service.name) ? "default" : "outline"}
            size="sm"
            onClick={() => toggleService(service.name)}
          >
            {service.name}
          </Button>
        ))}
      </div>
    </div>
  );
}
