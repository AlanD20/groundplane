import { controllerRequest } from "@/lib/controller-json-request";
import type { Tenant } from "@/lib/types";
import {
  tenantFromAPI,
  type TenantCreateRequest,
  type TenantCreateResponse,
  type TenantEditRequest,
  type TenantEditResponse,
  type TenantRenameRequest,
  type TenantRenameResponse,
} from "./api";
type HierarchyTaskAccepted = { task_id: string };
export type TenantActions = {
  addTenant: (t: {
    slug: string;
    name: string;
    description: string;
  }) => Promise<Tenant>;
  updateTenant: (
    slug: string,
    patch: { name: string; description: string },
  ) => Promise<Tenant>;
  renameTenant: (slug: string, nextSlug: string) => Promise<Tenant>;
  removeTenant: (tenantId: string) => Promise<string>;
};
export function createTenantActions(
  state: { tenants: Tenant[] },
  update: (change: (draft: { tenants: Tenant[] }) => void) => void,
): TenantActions {
  return {
    addTenant: async (tenant) => {
      const body: TenantCreateRequest = tenant;
      const created = tenantFromAPI(
        await controllerRequest<TenantCreateResponse>("/tenants", 201, {
          method: "POST",
          body,
        }),
      );
      update((draft) => {
        draft.tenants.push(created);
      });
      return created;
    },
    updateTenant: async (slug, patch) => {
      const current = state.tenants.find((tenant) => tenant.slug === slug);
      if (!current) throw new Error(`Tenant ${slug} no longer exists`);
      const body: TenantEditRequest = patch;
      const updated = tenantFromAPI(
        await controllerRequest<TenantEditResponse>(
          `/tenants/${encodeURIComponent(current.id)}`,
          200,
          { method: "PATCH", body },
        ),
      );
      update((draft) => {
        const index = draft.tenants.findIndex(
          (tenant) => tenant.id === updated.id,
        );
        if (index >= 0) draft.tenants[index] = updated;
      });
      return updated;
    },
    renameTenant: async (slug, nextSlug) => {
      const current = state.tenants.find((tenant) => tenant.slug === slug);
      if (!current) throw new Error(`Tenant ${slug} no longer exists`);
      const body: TenantRenameRequest = { slug: nextSlug };
      const renamed = tenantFromAPI(
        await controllerRequest<TenantRenameResponse>(
          `/tenants/${encodeURIComponent(current.id)}/rename`,
          200,
          { method: "POST", body },
        ),
      );
      update((draft) => {
        const index = draft.tenants.findIndex(
          (tenant) => tenant.id === renamed.id,
        );
        if (index >= 0) draft.tenants[index] = renamed;
      });
      return renamed;
    },
    removeTenant: async (tenantId) => {
      const accepted = await controllerRequest<HierarchyTaskAccepted>(
        `/tenants/${encodeURIComponent(tenantId)}`,
        202,
        { method: "DELETE" },
      );
      if (!accepted.task_id)
        throw new Error("Controller response is missing task_id");
      return accepted.task_id;
    },
  };
}
