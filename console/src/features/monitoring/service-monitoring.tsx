import { EmptyState } from "@/components/common/empty-state";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ServiceContainers } from "@/features/service/service-containers";
import { currentServiceObservation } from "@/features/service/service-observation";
import type { Service } from "@/lib/types";
import { Activity } from "lucide-react";
import { HealthOverview } from "./health-overview";
import { monitoringHealth } from "./health-summary";

export function ServiceMonitoring({
  service,
  now,
  onOpenLogs,
}: {
  service: Service;
  now: number;
  onOpenLogs: () => void;
}) {
  return (
    <div className="space-y-5">
      <HealthOverview summary={monitoringHealth([service], now)} />
      <ResourcePanel title="Resource usage">
        <EmptyState
          embedded
          icon={<Activity />}
          title="Resource telemetry is not available in this build"
          description="Runtime health and container observations are shown below. No CPU, memory, network, storage or disk I/O values are inferred from health."
        />
      </ResourcePanel>
      <ServiceContainers
        serviceId={service.id}
        serviceName={service.name}
        observation={currentServiceObservation(service.observation, now)}
        onOpenLogs={onOpenLogs}
      />
    </div>
  );
}
