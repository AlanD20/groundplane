import { Checkbox } from "@/components/ui/checkbox";

export function EntryProtection({
  secret,
  editing,
  onChange,
}: {
  secret: boolean;
  editing: boolean;
  onChange: (value: boolean) => void;
}) {
  return (
    <fieldset
      className="space-y-2 rounded-lg border border-border p-3"
      disabled={editing}
    >
      <legend className="px-1 text-sm font-medium">File contents</legend>
      <label className="flex items-center gap-2 text-sm">
        <Checkbox
          checked={secret}
          onChange={(event) => onChange(event.target.checked)}
        />
        Treat file contents as secret
      </label>
      <p className="text-sm text-muted-foreground">
        {editing
          ? "File classification is fixed when created."
          : secret
            ? "Values are encrypted at rest and hidden until explicitly revealed."
            : "Values remain visible in configuration."}
      </p>
    </fieldset>
  );
}
