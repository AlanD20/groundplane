import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { Service } from "@/lib/types";
import { replicaCountError } from "./deployment-readiness";

export function ReplicaCountField({
  strategy,
  value,
  onChange,
}: {
  strategy: Service["strategy"];
  value: string;
  onChange: (value: string) => void;
}) {
  const error = replicaCountError(strategy, Number(value));
  return (
    <div className="flex flex-col gap-1.5">
      <Label htmlFor="sv-replicas">Replicas</Label>
      <Input
        id="sv-replicas"
        type="number"
        min={1}
        max={
          strategy === "rolling"
            ? 16
            : strategy === "blue-green"
              ? 1
              : undefined
        }
        step={1}
        value={value}
        placeholder="1"
        aria-invalid={!!error}
        aria-describedby={error ? "sv-replicas-error" : undefined}
        onChange={(event) => onChange(event.target.value)}
      />
      {error && (
        <p
          id="sv-replicas-error"
          role="alert"
          className="text-xs text-destructive"
        >
          {error}
        </p>
      )}
    </div>
  );
}
