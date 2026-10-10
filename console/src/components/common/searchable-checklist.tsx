import { useState } from "react";
import { Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";

export type ChecklistOption = {
  id: string;
  label: string;
  detail?: string;
  searchText?: string;
  disabled?: boolean;
};

export function SearchableChecklist({
  label,
  options,
  selected,
  onChange,
  maximum,
}: {
  label: string;
  options: ChecklistOption[];
  selected: string[];
  onChange: (ids: string[]) => void;
  maximum: number;
}) {
  const [query, setQuery] = useState("");
  const visible = options.filter((option) =>
    `${option.label} ${option.detail ?? ""} ${option.searchText ?? ""}`
      .toLowerCase()
      .includes(query.trim().toLowerCase()),
  );
  return (
    <fieldset className="min-w-0 space-y-3">
      <legend className="text-sm font-medium">{label}</legend>
      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
        <span>
          {selected.length} selected · maximum {maximum}
        </span>
        <span>
          {options.filter((option) => !option.disabled).length} available
        </span>
      </div>
      <div className="relative">
        <Search
          aria-hidden
          className="pointer-events-none absolute left-3 top-3 size-4 text-muted-foreground"
        />
        <Input
          aria-label={`Search ${label.toLowerCase()}`}
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder="Search by name or path…"
          className="pl-9"
        />
      </div>
      <div className="max-h-64 overflow-y-auto rounded-lg border border-border bg-background divide-y divide-border">
        {visible.map((option) => {
          const checked = selected.includes(option.id);
          const disabled =
            !checked && (option.disabled || selected.length >= maximum);
          return (
            <label
              key={option.id}
              className={`flex items-start gap-3 p-3 ${disabled ? "cursor-not-allowed text-muted-foreground" : "cursor-pointer hover:bg-accent/40"}`}
            >
              <Checkbox
                checked={checked}
                disabled={disabled}
                className="mt-0.5 size-4 shrink-0"
                onChange={(event) =>
                  onChange(
                    event.target.checked
                      ? [...selected, option.id]
                      : selected.filter((item) => item !== option.id),
                  )
                }
              />
              <span className="min-w-0 space-y-1">
                <span className="block break-all font-mono text-xs">
                  {option.label}
                </span>
                {option.detail && (
                  <span className="block text-xs text-muted-foreground">
                    {option.detail}
                  </span>
                )}
              </span>
            </label>
          );
        })}
        {!visible.length && (
          <p className="p-3 text-sm text-muted-foreground">
            {query ? "No matches." : "Nothing available."}
          </p>
        )}
      </div>
    </fieldset>
  );
}
