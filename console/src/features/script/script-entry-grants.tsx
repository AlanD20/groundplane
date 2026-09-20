import type { EnvironmentEntry } from "@/lib/entry-types";
import { maximumScriptEntries } from "@/features/script/execution";
import { Checkbox } from "@/components/ui/checkbox";

export function ScriptEntryGrants({
  entries,
  service,
  selected,
  onChange,
}: {
  entries: EnvironmentEntry[];
  service: string;
  selected: string[];
  onChange: (ids: string[]) => void;
}) {
  const eligible = entries.filter(
    (entry) =>
      entry.exposure.includes("all") || entry.exposure.includes(service),
  );
  const options = eligible.map((entry) => ({
    id: entry.id,
    label: entry.reconciliationKey ?? entry.key ?? entry.path ?? entry.id,
    detail: `${entry.type}${entry.secret ? " · secret" : ""}`,
    available: true,
  }));
  for (const id of selected) {
    if (options.some((option) => option.id === id)) continue;
    const entry = entries.find((candidate) => candidate.id === id);
    options.push({
      id,
      label: entry?.reconciliationKey ?? entry?.key ?? entry?.path ?? id,
      detail: "saved grant · unavailable or not exposed to this Service",
      available: false,
    });
  }
  return (
    <fieldset className="flex min-w-0 flex-col gap-2">
      <legend className="mb-2 text-sm font-medium">
        Entry grants ({selected.length}/{maximumScriptEntries})
      </legend>
      <div className="flex max-h-48 flex-col gap-2 overflow-y-auto rounded-lg border border-border p-3">
        {options.map((entry) => {
          const checked = selected.includes(entry.id);
          return (
            <label
              key={entry.id}
              className="flex cursor-pointer items-start gap-2 text-sm"
            >
              <Checkbox
                checked={checked}
                className="mt-0.5 size-4 shrink-0 accent-primary"
                disabled={
                  !checked &&
                  (!entry.available || selected.length >= maximumScriptEntries)
                }
                onChange={(event) =>
                  onChange(
                    event.target.checked
                      ? [...selected, entry.id]
                      : selected.filter((id) => id !== entry.id),
                  )
                }
              />
              <span className="flex min-w-0 flex-col">
                <span className="break-all font-mono text-xs">
                  {entry.label}
                </span>
                <span className="text-xs text-muted-foreground">
                  {entry.detail}
                </span>
              </span>
            </label>
          );
        })}
        {options.length === 0 && (
          <p className="text-xs text-muted-foreground">
            No Entries are exposed to this Service.
          </p>
        )}
      </div>
      <p className="text-xs text-muted-foreground">
        Only selected Entries are available. Secret values are never displayed
        here.
      </p>
    </fieldset>
  );
}
