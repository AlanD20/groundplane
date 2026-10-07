import { Badge } from "@/components/ui/badge";
import { EmptySecretValueBadge } from "@/components/common/empty-secret-value-badge";
import type { BlueprintValidationResponse } from "./api";

export function BlueprintReview({
  validation,
}: {
  validation: BlueprintValidationResponse;
}) {
  const changes = validation.changes ?? [];
  const removedEntries = changes.filter(
    (change) => change.resource === "entry" && change.action === "remove",
  );

  return (
    <div className="space-y-3 text-sm">
      <p className="text-xs text-muted-foreground">
        {validation.service ? `Apply only ${validation.service}. Other Services and shared resources remain unchanged.` : 'Apply the complete Environment.'}
        {' '}Running Services use the strategy shown below; recreate causes downtime.
      </p>
      {validation.revision !== "0" && (
        <p className="rounded-lg border border-warning/30 bg-warning/5 p-3">
          This Blueprint changes the Environment’s intended configuration.
          Review the effects below before applying it to existing resources.
        </p>
      )}
      {removedEntries.length > 0 && (
        <p
          role="alert"
          className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-destructive"
        >
          Removing an Entry from the Blueprint removes its managed configuration
          from the Environment. Running processes may retain their loaded values
          until their next deployment.
        </p>
      )}
      <div className="max-h-64 space-y-1 overflow-y-auto rounded-lg border border-border bg-surface p-3 text-xs">
        {changes.map((change) => (
          <div
            key={`${change.resource}:${change.key}:${change.action}`}
            className="flex gap-2"
          >
            <Badge variant="outline">{change.action}</Badge>
            {change.release_strategy && <Badge variant="outline">{change.release_strategy}</Badge>}
            <code>
              {change.resource}/{change.key}
            </code>
            <EmptySecretValueBadge empty={change.empty_secret_value} />
          </div>
        ))}
        {changes.length === 0 && <p>No resource changes.</p>}
      </div>
    </div>
  );
}
