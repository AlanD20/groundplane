"use client";

export function parseBulkEnvironmentEntries(
  input: string,
): { key: string; value: string }[] {
  const entries: { key: string; value: string }[] = [];
  const seen = new Set<string>();
  input
    .replaceAll("\r\n", "\n")
    .split("\n")
    .forEach((rawLine, index) => {
      const line = rawLine.endsWith("\r") ? rawLine.slice(0, -1) : rawLine;
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) return;
      const separator = line.indexOf("=");
      if (separator < 0)
        throw new Error(`Line ${index + 1} must use KEY=value.`);
      const key = line.slice(0, separator).trim();
      if (!key) throw new Error(`Line ${index + 1} has an empty key.`);
      if (seen.has(key))
        throw new Error(`Line ${index + 1} duplicates ${key}.`);
      seen.add(key);
      let value = line.slice(separator + 1);
      if (value.startsWith('"')) {
        try {
          value = JSON.parse(value);
          if (typeof value !== "string") throw new Error();
        } catch {
          throw new Error(`Line ${index + 1} has an invalid quoted value.`);
        }
      }
      entries.push({ key, value });
    });
  if (entries.length === 0)
    throw new Error("Enter at least one KEY=value line.");
  return entries;
}

export function formatBulkEnvironmentEntries(
  entries: { key: string; value: string }[],
) {
  return entries
    .map(
      ({ key, value }) =>
        `${key}=${/[\r\n]/.test(value) || value.startsWith('"') ? JSON.stringify(value) : value}`,
    )
    .join("\n");
}
