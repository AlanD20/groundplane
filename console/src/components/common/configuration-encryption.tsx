import { Checkbox } from "@/components/ui/checkbox";

export function ConfigurationEncryption({
  value, onChange, sensitive, disabled = false,
}: {
  value: "age" | "none";
  onChange: (value: "age" | "none") => void;
  sensitive: boolean;
  disabled?: boolean;
}) {
  return (
    <div className="space-y-2">
      <label className="flex items-center gap-2 text-sm font-medium">
        <Checkbox checked={value === "age"} disabled={disabled}
          onChange={(event) => onChange(event.target.checked ? "age" : "none")} />
        Encrypt stored value
      </label>
      <p className="text-xs text-muted-foreground">
        {value === "age" ? "Encrypted at rest in the Controller." : "Stored unencrypted in the Controller."}{" "}
        {sensitive ? "Hidden until explicitly revealed, regardless of encryption." : "Not classified as secret."}
      </p>
    </div>
  );
}
