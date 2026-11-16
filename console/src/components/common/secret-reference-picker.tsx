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
  noun = "Secret",
}: {
  id: string;
  value: string;
  options: SelectOption[];
  onChange: (value: string) => void;
  noun?: string;
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
          placeholder={`Enter ${noun} reference`}
          onChange={(event) => onChange(event.target.value)}
        />
      ) : (
        <SearchableSelect
          id={id}
          aria-label={`Reusable ${noun}`}
          value={value || null}
          options={choices}
          onValueChange={onChange}
          placeholder={`Choose ${noun}`}
          searchPlaceholder={`Search ${noun}…`}
          emptyText={`No ${noun} available. Enter a reference manually.`}
        />
      )}
      <Button
        type="button"
        size="sm"
        variant="link"
        onClick={() => setManual(!manual)}
      >
        {manual ? `Choose existing ${noun}` : "Enter reference manually"}
      </Button>
    </div>
  );
}
