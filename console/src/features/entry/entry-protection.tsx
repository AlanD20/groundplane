import { Button } from "@/components/ui/button";

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
      <legend className="px-1 text-sm font-medium">Protection</legend>
      <div className="flex gap-2">
        <Button
          type="button"
          variant={!secret ? "default" : "outline"}
          aria-pressed={!secret}
          onClick={() => onChange(false)}
        >
          Plain
        </Button>
        <Button
          type="button"
          variant={secret ? "default" : "outline"}
          aria-pressed={secret}
          onClick={() => onChange(true)}
        >
          Encrypted
        </Button>
      </div>
      <p className="text-sm text-muted-foreground">
        {editing
          ? "Protection is fixed when created. Create a new Entry to change it."
          : secret
            ? "Values are encrypted at rest and hidden until explicitly revealed."
            : "Values remain visible in configuration."}
      </p>
    </fieldset>
  );
}
