import type { Service } from "@/lib/types";
import { healthcheckEnabled } from "./healthcheck";
import { portInputError, trafficSwitchPortError } from "./port-input";

export function replicaCountError(
  strategy: Service["strategy"],
  value: number,
) {
  if (!Number.isInteger(value) || value < 1)
    return "Enter a positive whole replica count.";
  if (strategy === "rolling" && value > 16)
    return "Rolling supports 1–16 replicas.";
  if (strategy === "blue-green" && value !== 1)
    return "Blue-green requires one replica. Use rolling or recreate for multiple replicas.";
  return undefined;
}

export function deploymentReadinessError(
  service: Service,
  strategy: Service["strategy"],
) {
  const replicas = replicaCountError(strategy, service.replicas);
  if (replicas) return replicas;
  const ports = portInputError(service.expose.join(","));
  if (ports) return ports;
  if (strategy !== "recreate") {
    const switching = trafficSwitchPortError(service.expose);
    if (switching) return switching;
  }
  if (strategy !== "recreate" && !healthcheckEnabled(service.healthcheck))
    return "Configure an enabled healthcheck before deploying with this strategy, or select recreate.";
  if (strategy === "blue-green" && service.expose.length === 0)
    return "Blue-green requires an exposed TCP port. Configure one or use rolling for a portless worker.";
  return undefined;
}
