import type { operations } from "@/lib/api.generated";
import type { Tenant } from "@/lib/types";
import { controllerRequest } from "@/lib/controller-json-request";
export type TenantPageResponse =
  operations["tenant.list"]["responses"][200]["content"]["application/json"];
export type TenantCreateRequest =
  operations["tenant.create"]["requestBody"]["content"]["application/json"];
export type TenantCreateResponse =
  operations["tenant.create"]["responses"][201]["content"]["application/json"];
export type TenantEditRequest =
  operations["tenant.edit"]["requestBody"]["content"]["application/json"];
export type TenantEditResponse =
  operations["tenant.edit"]["responses"][200]["content"]["application/json"];
export type TenantRenameRequest =
  operations["tenant.rename"]["requestBody"]["content"]["application/json"];
export type TenantRenameResponse =
  operations["tenant.rename"]["responses"][200]["content"]["application/json"];
export function tenantFromAPI(tenant: TenantCreateResponse): Tenant {
  return {
    id: tenant.id,
    slug: tenant.slug,
    name: tenant.name,
    description: tenant.description,
    deletionTaskId: tenant.deletion_task_id,
  };
}

export async function listAllTenants(signal: AbortSignal): Promise<Tenant[]> {
  const tenants: Tenant[] = [];
  let cursor = "";
  do {
    const query = new URLSearchParams({ limit: "200" });
    if (cursor) query.set("cursor", cursor);
    const page = await controllerRequest<TenantPageResponse>(
      `/tenants?${query}`,
      200,
      { signal },
    );
    tenants.push(...(page.items ?? []).map(tenantFromAPI));
    cursor = page.next_cursor ?? "";
  } while (cursor);
  return tenants;
}
