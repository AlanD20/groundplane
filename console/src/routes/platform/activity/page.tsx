"use client";

import { PageHeader } from "@/components/common/page-header";
import { Select } from "@/components/ui/select";
import { TaskList } from "@/features/task/task-list";
import { useStore } from "@/lib/store";
import type { TaskJournalScope } from "@/lib/types";
import { Activity } from "lucide-react";
import { useMemo } from "react";
import { useSearchParams } from "react-router-dom";

function activityScope(filter: string): TaskJournalScope {
  if (filter === "platform")
    return { kind: "workspace", workspace: "platform" };
  const separator = filter.indexOf(":");
  if (separator < 0) return { kind: "all" };
  const kind = filter.slice(0, separator);
  const id = filter.slice(separator + 1);
  if (kind === "tenant") return { kind: "workspace", workspace: id };
  if (kind === "project") return { kind: "project", projectId: id };
  if (kind === "environment") return { kind: "environment", environmentId: id };
  return { kind: "all" };
}

export default function PlatformActivityPage() {
  const store = useStore();
  const [search, setSearch] = useSearchParams();
  const filter = search.get("owner") ?? "all";
  const scope = useMemo(() => activityScope(filter), [filter]);
  const options = useMemo(() => {
    const result = [
      { value: "all", label: "All activity" },
      { value: "platform", label: "Platform" },
    ];
    for (const tenant of store.tenants) {
      result.push({
        value: `tenant:${tenant.id}`,
        label: `Tenant / ${tenant.slug}`,
      });
    }
    for (const project of [...store.tenantProjects, ...store.backingProjects]) {
      const tenant = store.tenants.find(
        (candidate) => candidate.id === project.tenantId,
      );
      const owner = tenant?.slug ?? "backing";
      result.push({
        value: `project:${project.id}`,
        label: `Project / ${owner} / ${project.slug}`,
      });
      for (const environment of project.environments ?? []) {
        result.push({
          value: `environment:${environment.id}`,
          label: `Environment / ${owner} / ${project.slug} / ${environment.name}`,
        });
      }
    }
    return result;
  }, [store.backingProjects, store.tenantProjects, store.tenants]);

  return (
    <div className="flex flex-col gap-6">
      <PageHeader title="Activity" icon={<Activity />} />
      <TaskList
        scope={scope}
        surface="activity"
        title="All Tasks"
        filters={
          <Select
            aria-label="Activity owner"
            className="w-64 max-w-full"
            value={filter}
            onValueChange={(value) => {
              setSearch(
                (current) => {
                  const next = new URLSearchParams(current);
                  next.set("owner", value);
                  return next;
                },
                { replace: true },
              );
            }}
            options={options}
          />
        }
      />
    </div>
  );
}
