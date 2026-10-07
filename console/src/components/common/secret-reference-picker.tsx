import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { SearchableSelect } from "@/components/ui/searchable-select";
import type { SelectOption } from "@/components/ui/select";

export function SecretReferencePicker({
  id,
  value,
  options,
  onChange,
}: {
  id: string;
  value: string;
  options: SelectOption[];
  onChange: (value: string) => void;
}) {
  const [manual, setManual] = useState(false);
  const choices =
    options.some((option) => option.value === value) || !value
      ? options
      : [{ value, label: `${value} · current reference` }, ...options];
  return (
    <div className="min-w-0 space-y-2">
      {manual ? (
        <Input
          id={id}
          value={value}
          autoComplete="off"
          placeholder="Enter Secret reference"
          onChange={(event) => onChange(event.target.value)}
        />
      ) : (
        <SearchableSelect
          id={id}
          aria-label="Reusable Secret"
          value={value || null}
          options={choices}
          onValueChange={onChange}
          placeholder="Choose a Secret"
          searchPlaceholder="Search Secrets…"
          emptyText="No Secrets available. Enter a reference manually."
        />
      )}
      <Button
        type="button"
        size="sm"
        variant="link"
        onClick={() => setManual(!manual)}
      >
        {manual ? "Choose an existing Secret" : "Enter reference manually"}
      </Button>
    </div>
  );
}
