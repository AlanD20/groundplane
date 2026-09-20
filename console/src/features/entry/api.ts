import type { operations } from "@/lib/api.generated";
import type { EnvironmentEntry } from "@/lib/types";
import { entryFromAPI } from "@/lib/entry-api";
import { controllerRequest } from "@/lib/controller-json-request";
export type EntryPageResponse =
  operations["entry.list"]["responses"][200]["content"]["application/json"];
export type EntryResponse =
  operations["entry.edit"]["responses"][200]["content"]["application/json"];
export type EntryCreateRequest =
  operations["entry.create"]["requestBody"]["content"]["application/json"];
export type EntryEditRequest =
  operations["entry.edit"]["requestBody"]["content"]["application/json"];
export type EntryBulkUpsertRequest =
  operations["entry.bulk-upsert"]["requestBody"]["content"]["application/json"];
export type EntryBulkUpsertResponse =
  operations["entry.bulk-upsert"]["responses"][202]["content"]["application/json"];
export type EntryValueResponse =
  operations["entry.reveal"]["responses"][200]["content"]["application/json"];
export async function listAllEntries(
  environmentId: string,
  signal?: AbortSignal,
): Promise<EnvironmentEntry[]> {
  const entries: EnvironmentEntry[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<EntryPageResponse>(
      `/entries?${query}`,
      200,
      { signal },
    );
    entries.push(...(page.items ?? []).map(entryFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return entries;
}
