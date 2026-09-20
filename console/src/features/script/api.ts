import type { operations } from "@/lib/api.generated";
import type { Script } from "@/features/script/types";
import { scriptFromAPI } from "@/features/script/request-model";
import { controllerRequest } from "@/lib/controller-json-request";
type ScriptPageResponse =
  operations["script.list"]["responses"][200]["content"]["application/json"];
export async function listAllScripts(
  environmentId: string,
  signal?: AbortSignal,
): Promise<Script[]> {
  const scripts: Script[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({
      environment: environmentId,
      limit: "200",
    });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<ScriptPageResponse>(
      `/scripts?${query}`,
      200,
      { signal },
    );
    scripts.push(...(page.items ?? []).map(scriptFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return scripts;
}
