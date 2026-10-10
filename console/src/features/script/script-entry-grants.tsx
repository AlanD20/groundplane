import type { EnvironmentEntry } from "@/lib/entry-types";
import { maximumScriptEntries } from "@/features/script/execution";
import { SearchableChecklist } from "@/components/common/searchable-checklist";

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
  const options = entries.map((entry) => {
    const available =
      entry.exposure.includes("all") || entry.exposure.includes(service);
    return {
      id: entry.id,
      label: entry.key ?? entry.path ?? entry.reconciliationKey ?? entry.id,
      searchText: `${entry.reconciliationKey ?? ""} ${entry.id}`,
      detail: `${entry.type === "env" ? "Variable" : "File"}${entry.secret ? " · encrypted" : ""}${available ? "" : ` · Not exposed to ${service || "the selected Service"}`}`,
      disabled: !available,
    };
  });
  for (const id of selected) {
    if (options.some((option) => option.id === id)) continue;
    const entry = entries.find((candidate) => candidate.id === id);
    options.push({
      id,
      label: entry?.reconciliationKey ?? entry?.key ?? entry?.path ?? id,
      detail: "Unavailable or not exposed to this Service",
      disabled: true,
      searchText: id,
    });
  }
  return (
    <div className="space-y-2">
      <SearchableChecklist
        label="Variables & files"
        options={options}
        selected={selected}
        onChange={onChange}
        maximum={maximumScriptEntries}
      />
      <p className="text-xs text-muted-foreground">
        Selected variables and files are made available to this Script.
      </p>
    </div>
  );
}
