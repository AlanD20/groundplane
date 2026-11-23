import { CreationButton } from "@/components/common/creation-button";
import { InspectionWorkspace } from "@/components/common/inspection-workspace";
import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { buttonVariants } from "@/components/ui/button";
("use client");

import { useState } from "react";
import { Link } from "react-router-dom";
import { ChevronRight, Plus, Workflow } from "lucide-react";
import { EmptyState } from "@/components/common/empty-state";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useRequiredParams } from "@/lib/router";
import type { Environment } from "@/lib/types";
import { ReleaseGroupFormDrawer } from "./release-group-form-drawer";
import {
  releaseGroupPath,
  releaseGroupPolicyLabel,
  releaseGroupTag,
} from "./release-group-projections";

export function ReleaseGroupsPanel({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant", "project", "env");
  const [adding, setAdding] = useState(false);
  const [selectedId, setSelectedId] = useState<string>();
  const [query, setQuery] = useState("");
  const table = useTableView(
    env.releaseGroups.filter((group) =>
      [
        group.name,
        ...group.order,
        releaseGroupTag(env, group),
        releaseGroupPolicyLabel(group.onFailure),
      ]
        .join(" ")
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
    ),
    { name: (group) => group.name, members: (group) => group.order.length },
    "name",
    "asc",
    `${env.id}/${query}`,
  );
  const selected =
    table.rows.find((group) => group.id === selectedId) ?? table.rows[0];

  return (
    <Card>
      <CardHeader className="flex-row items-center justify-between gap-3">
        <div className="flex flex-col gap-1">
          <CardTitle className="flex items-center gap-2">
            <Workflow className="size-4 text-primary" /> Deployment groups
            <Badge variant="outline">{env.releaseGroups.length}</Badge>
          </CardTitle>
        </div>
        <CreationButton
          active={adding}
          onOpenChange={setAdding}
          disabled={env.services.length < 2}
        >
          <Plus className="size-4" /> Add group
        </CreationButton>
      </CardHeader>
      <CardContent>
        <ReleaseGroupFormDrawer
          env={env}
          open={adding}
          onOpenChange={setAdding}
        />

        {env.services.length < 2 ? (
          <EmptyState
            embedded
            icon={<Workflow />}
            title="At least two services are required"
            description="Add another service before defining a coordinated deployment group."
          />
        ) : env.releaseGroups.length === 0 ? (
          <EmptyState
            embedded
            icon={<Workflow />}
            title="No deployment groups"
            description="Deploy Services in a declared order under one Task and failure policy."
          />
        ) : (
          <InspectionWorkspace
            title="Group list"
            selectionLabel="Select deployment group"
            detailsLabel="Selected deployment group"
            emptyMessage="No deployment groups match your search."
            selectedId={selected?.id}
            onSelect={setSelectedId}
            items={table.rows.map((group) => ({
              id: group.id,
              label: group.name,
              description: `${group.order.length} ${group.order.length === 1 ? "service" : "services"}`,
            }))}
            toolbar={
              <ListToolbar
                label="deployment groups"
                query={query}
                onQueryChange={setQuery}
                sort={table}
                fields={[
                  { value: "name", label: "Name" },
                  { value: "members", label: "Service count" },
                ]}
              />
            }
            footer={
              table.pages > 1 && (
                <TablePagination table={table} label="Deployment groups" />
              )
            }
          >
            {selected && (
              <div className="space-y-4">
                <header className="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-3">
                  <div className="min-w-0 space-y-1">
                    <h3 className="text-sm font-semibold [overflow-wrap:anywhere]">
                      {selected.name}
                    </h3>
                    <p className="text-xs text-muted-foreground">
                      {releaseGroupPolicyLabel(selected.onFailure)}
                    </p>
                  </div>
                  <Link
                    className={buttonVariants({
                      variant: "outline",
                      size: "sm",
                    })}
                    to={releaseGroupPath(
                      params.tenant,
                      params.project,
                      params.env,
                      selected.id,
                    )}
                  >
                    Open group <ChevronRight aria-hidden className="size-3.5" />
                  </Link>
                </header>
                <div className="space-y-2">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <h4 className="text-sm font-semibold">Release order</h4>
                    <Badge variant="primary">
                      {releaseGroupTag(env, selected)}
                    </Badge>
                  </div>
                  <ol className="divide-y divide-border rounded-lg border border-border">
                    {selected.order.map((service, index) => (
                      <li
                        key={service}
                        className="flex min-w-0 items-center gap-2 px-3 py-2 text-sm"
                      >
                        <span className="min-w-6 text-xs tabular-nums text-muted-foreground">
                          {index + 1}
                        </span>
                        <span className="min-w-0 [overflow-wrap:anywhere]">
                          {service}
                        </span>
                      </li>
                    ))}
                  </ol>
                </div>
              </div>
            )}
          </InspectionWorkspace>
        )}
      </CardContent>
    </Card>
  );
}
