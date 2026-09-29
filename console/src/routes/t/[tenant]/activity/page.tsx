"use client";

import { EmptyState } from "@/components/common/empty-state";
import { PageHeader } from "@/components/common/page-header";
import { Select } from "@/components/ui/select";
import { TaskList } from "@/features/task/task-list";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { TaskJournalScope } from "@/lib/types";
import { Activity } from "lucide-react";
import { useMemo, useState } from "react";

export default function TenantActivityPage() {
  const params = useRequiredParams("tenant");
  const store = useStore();
  const tenant = store.getTenant(params.tenant);
  const [filter, setFilter] = useState("all");
  const projects = useMemo(
    () =>
      tenant
        ? store.tenantProjects.filter(
            (project) => project.tenantId === tenant.id,
          )
        : [],
    [store.tenantProjects, tenant?.id],
  );
  const options = useMemo(() => {
    const result = [{ value: "all", label: "All tenant activity" }];
    for (const project of projects) {
      result.push({
        value: `project:${project.id}`,
        label: `Project / ${project.slug}`,
      });
      for (const environment of project.environments ?? []) {
        result.push({
          value: `environment:${environment.id}`,
          label: `Environment / ${project.slug} / ${environment.name}`,
        });
      }
    }
    return result;
  }, [projects]);
  const effectiveFilter = options.some((option) => option.value === filter)
    ? filter
    : "all";
  const scope = useMemo<TaskJournalScope | null>(() => {
    if (!tenant) return null;
    const separator = effectiveFilter.indexOf(":");
    if (separator < 0) return { kind: "workspace", workspace: tenant.id };
    const kind = effectiveFilter.slice(0, separator);
    const id = effectiveFilter.slice(separator + 1);
    if (kind === "project") return { kind: "project", projectId: id };
    if (kind === "environment")
      return { kind: "environment", environmentId: id };
    return { kind: "workspace", workspace: tenant.id };
  }, [effectiveFilter, tenant?.id]);

  if (store.tenantsLoading) {
    return (
      <div
        role="status"
        className="py-10 text-center text-sm text-muted-foreground"
      >
        loading tenant…
      </div>
    );
  }
  if (store.tenantError) {
    return (
      <div
        role="alert"
        className="rounded-lg border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive"
      >
        {store.tenantError}
      </div>
    );
  }
  if (!tenant)
    return <EmptyState icon={<Activity />} title="Tenant not found" />;
  if (!scope) return null;

  return (
    <div className="flex flex-col gap-6">
      <PageHeader title="Activity" icon={<Activity />} />
      <TaskList
        scope={scope}
        surface="activity"
        title="Tenant Tasks"
        filters={
          <Select
            aria-label="Activity owner"
            className="w-64 max-w-full"
            value={effectiveFilter}
            onValueChange={setFilter}
            options={options}
          />
        }
      />
    </div>
  );
}
