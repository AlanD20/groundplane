import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function rollingTimeoutError(value: string): string | undefined {
  const seconds = Number(value);
  return Number.isInteger(seconds) && seconds >= 0 && seconds <= 86400
    ? undefined
    : "Enter whole seconds from 0 to 86400.";
}

export function RollingTimeoutField({
  value,
  onChange,
}: {
  value: string;
  onChange: (value: string) => void;
}) {
  const error = rollingTimeoutError(value);
  return (
    <div className="flex flex-col gap-1.5 sm:col-span-2">
      <Label htmlFor="sv-rolling-timeout">
        Rolling replica timeout (seconds)
      </Label>
      <Input
        id="sv-rolling-timeout"
        type="number"
        min={0}
        max={86400}
        step={1}
        value={value}
        placeholder="300"
        aria-invalid={!!error}
        aria-describedby="sv-rolling-timeout-help"
        onChange={(event) => onChange(event.target.value)}
      />
      <p
        id="sv-rolling-timeout-help"
        className={
          error ? "text-xs text-destructive" : "text-xs text-muted-foreground"
        }
      >
        {error ??
          "Time for one replica's startup, healthcheck, traffic switch and retirement. Empty or 0 uses 300 seconds; the Task deadline still applies."}
      </p>
    </div>
  );
}
