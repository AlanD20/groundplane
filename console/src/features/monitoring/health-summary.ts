import type { Service } from "@/lib/types";
import { currentServiceObservation } from "../service/service-observation.ts";

type HealthSource = Pick<
  Service,
  "id" | "name" | "runtimeIntent" | "observation"
>;

export function monitoringHealth<T extends HealthSource>(
  services: readonly T[],
  now: number,
) {
  const rows = services.map((service) => {
    const observation = currentServiceObservation(service.observation, now);
    const state = observation.state;
    const attention =
      state === "degraded" ||
      state === "failed" ||
      (service.runtimeIntent === "running" &&
        (state === "stopped" || state === "absent"));
    const sampledAt =
      service.observation.state === "unavailable"
        ? undefined
        : service.observation.observedAt;
    return { service, observation, state, attention, sampledAt };
  });
  const healthy = rows.filter((row) => row.state === "healthy").length;
  const running = rows.filter((row) => row.state === "running").length;
  const starting = rows.filter((row) => row.state === "starting").length;
  const unavailable = rows.filter((row) => row.state === "unavailable").length;
  const attention = rows.filter((row) => row.attention).length;
  const reported = rows.length - unavailable;
  const title =
    attention > 0
      ? rows.length === 1
        ? "Service needs attention"
        : "Some Services need attention"
      : unavailable > 0
        ? "Waiting for fresh observations"
        : starting > 0
          ? rows.length === 1
            ? "Service is starting"
            : "Services are starting"
          : "No runtime issues observed";
  const tone =
    attention > 0
      ? "degraded"
      : unavailable > 0
        ? "unavailable"
        : starting > 0
          ? "starting"
          : "healthy";
  return {
    rows,
    healthy,
    running,
    starting,
    unavailable,
    attention,
    reported,
    total: rows.length,
    title,
    tone,
  };
}
