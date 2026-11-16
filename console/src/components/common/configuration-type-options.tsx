import { Button } from "@/components/ui/button";

export type ConfigurationType = "variable" | "secret" | "file";

export function ConfigurationTypeOptions({
  value,
  onChange,
  disabled = false,
  files = true,
}: {
  value: ConfigurationType;
  onChange: (value: ConfigurationType) => void;
  disabled?: boolean;
  files?: boolean;
}) {
  return (
    <fieldset disabled={disabled} className="space-y-2">
      <legend className="mb-2 text-sm font-medium">Type</legend>
      <div className="flex flex-wrap gap-2">
        {(
          [
            "variable",
            "secret",
            ...(files ? ["file"] : []),
          ] as ConfigurationType[]
        ).map((candidate) => (
          <Button
            type="button"
            key={candidate}
            size="sm"
            variant={candidate === value ? "default" : "outline"}
            aria-pressed={candidate === value}
            onClick={() => onChange(candidate)}
          >
            {candidate === "variable"
              ? "Variable"
              : candidate === "secret"
                ? "Secret"
                : "File"}
          </Button>
        ))}
      </div>
    </fieldset>
  );
}
