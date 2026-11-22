import { ServiceContainers } from "@/features/service/service-containers";
import { currentServiceObservation } from "@/features/service/service-observation";
import type { Service } from "@/lib/types";
import type { MonitoringScopeKind } from "./api";
import { HealthOverview } from "./health-overview";
import { monitoringHealth } from "./health-summary";
import { ScopedMonitoring } from "./scoped-monitoring";

export function ServiceMonitoring({
  service,
  now,
  onOpenLogs,
  scope = "service",
  scopeID = service.id,
  scopeName = service.name,
}: {
  service: Service;
  now: number;
  onOpenLogs: () => void;
  scope?: Extract<MonitoringScopeKind, "service" | "backing">;
  scopeID?: string;
  scopeName?: string;
}) {
  return (
    <div className="space-y-5">
      <HealthOverview summary={monitoringHealth([service], now)} />
      <ScopedMonitoring scope={scope} id={scopeID} name={scopeName} />
      <ServiceContainers
        serviceId={service.id}
        serviceName={service.name}
        observation={currentServiceObservation(service.observation, now)}
        onOpenLogs={onOpenLogs}
      />
    </div>
  );
}
