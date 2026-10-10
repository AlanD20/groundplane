import type { EnvironmentEntry } from "@/lib/entry-types";

export type BulkEntryValue = {
  entry: EnvironmentEntry;
  key: string;
  value: string;
};
export type BulkEntryBatch = {
  entries: { key: string; value: string }[];
  secret: boolean;
  exposure: string[];
};

export async function loadBulkEntryValues(
  entries: EnvironmentEntry[],
  reveal: (id: string) => Promise<string>,
): Promise<BulkEntryValue[]> {
  const variables = entries.filter((entry) => entry.type === "env");
  const values: BulkEntryValue[] = [];
  for (let offset = 0; offset < variables.length; offset += 8) {
    values.push(
      ...(await Promise.all(
        variables.slice(offset, offset + 8).map(async (entry) => ({
          entry,
          key: entry.key!,
          value:
            entry.source.kind === "literal" && !entry.secret
              ? (entry.source.literal ?? "")
              : await reveal(entry.id),
        })),
      )),
    );
  }
  return values;
}

// Existing keys retain their own protection and exposure. Only additions use defaults.
export function planBulkEntryChanges(
  values: { key: string; value: string }[],
  baseline: BulkEntryValue[],
  defaults: { secret: boolean; exposure: string[] },
  outsideKeys: string[] = [],
) {
  const existing = new Map(baseline.map((item) => [item.key, item]));
  const batches: BulkEntryBatch[] = [];
  const replacedReferences: string[] = [];
  for (const value of values) {
    const prior = existing.get(value.key);
    if (prior?.value === value.value) continue;
    if (!prior && outsideKeys.includes(value.key)) {
      throw new Error(
        `${value.key} belongs to another Service. Edit it from the Environment.`,
      );
    }
    if (prior && prior.entry.source.kind !== "literal")
      replacedReferences.push(value.key);
    const secret = prior?.entry.secret ?? defaults.secret;
    const exposure = prior?.entry.exposure ?? defaults.exposure;
    const group = JSON.stringify([...exposure].sort());
    let batch = batches.find(
      (item) =>
        item.secret === secret &&
        JSON.stringify([...item.exposure].sort()) === group &&
        item.entries.length < 200,
    );
    if (!batch) {
      batch = { entries: [], secret, exposure: [...exposure] };
      batches.push(batch);
    }
    batch.entries.push(value);
  }
  return { batches, replacedReferences };
}
