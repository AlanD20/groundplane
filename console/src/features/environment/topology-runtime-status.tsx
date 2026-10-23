import { StatusBadge } from "@/components/common/status-badge";
import { Tooltip } from "@/components/ui/tooltip";
import {
  currentServiceObservation,
  replicaTotal,
  type ServiceObservation,
} from "@/features/service/service-observation";

export function TopologyRuntimeStatus({
  observation,
  now,
}: {
  observation: ServiceObservation;
  now: number;
}) {
  const current = currentServiceObservation(observation, now);
  return (
    <Tooltip
      content={
        <div className="space-y-1">
          {current.state === "unavailable" ? (
            <p>No current runtime observation. Health is unavailable.</p>
          ) : (
            <>
              <p>
                {replicaTotal(current.replicas)}/{current.expectedReplicas}{" "}
                replicas observed · {current.replicas.healthy} healthy
              </p>
              {current.containers.map((container) => (
                <p key={container.id} className="break-all">
                  {container.name}: {container.state} ·{" "}
                  {container.health === "none"
                    ? "no healthcheck"
                    : container.health}
                </p>
              ))}
            </>
          )}
          {observation.state !== "unavailable" && (
            <p>
              Last observed: {new Date(observation.observedAt).toLocaleString()}
            </p>
          )}
        </div>
      }
    >
      <span className="inline-flex shrink-0" tabIndex={0}>
        <StatusBadge
          status={current.state}
          label={
            current.state === "running" ? "Running · unchecked" : undefined
          }
          className="text-[10px]"
        />
      </span>
    </Tooltip>
  );
}
