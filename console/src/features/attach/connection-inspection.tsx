import { InspectionWorkspace } from "@/components/common/inspection-workspace";
import { EmptyState } from "@/components/common/empty-state";
import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { StatusBadge } from "@/components/common/status-badge";
import { useStore } from "@/lib/store";
import type { Attach } from "@/lib/types";
import { Database, Plug } from "lucide-react";
import { useState, type ReactNode } from "react";

export function ConnectionInspection({
  attaches,
  scope,
  showValues = false,
  children,
}: {
  attaches: Attach[];
  scope: string;
  showValues?: boolean;
  children: (attach: Attach, backingName: string) => ReactNode;
}) {
  const { getBackingProject } = useStore();
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string>();
  const rows = attaches.map((attach) => ({
    attach,
    backingName: getBackingProject(attach.projectId)?.name || attach.projectId,
    valueCount: attach.factSets.reduce(
      (count, set) => count + set.facts.length,
      0,
    ),
  }));
  const needle = query.trim().toLowerCase();
  const table = useTableView(
    rows.filter(({ attach, backingName }) =>
      [
        attach.name,
        attach.service,
        attach.database,
        backingName,
        ...(showValues
          ? attach.factSets.flatMap((set) => set.facts.map((fact) => fact.key))
          : []),
      ]
        .join(" ")
        .toLowerCase()
        .includes(needle),
    ),
    {
      name: ({ attach }) => attach.name,
      service: ({ attach }) => attach.service,
      backing: ({ backingName }) => backingName,
    },
    "name",
    "asc",
    `${scope}/${query}`,
  );
  const selected =
    table.rows.find(({ attach }) => attach.id === selectedId) ?? table.rows[0];

  if (!attaches.length)
    return (
      <EmptyState
        embedded
        icon={showValues ? <Database /> : <Plug />}
        title={
          showValues ? "No connection values yet" : "No backing connections yet"
        }
        description="Connect a Service to a backing service to make its connection values available."
      />
    );

  return (
    <InspectionWorkspace
      title="Connections"
      selectionLabel="Select connection"
      detailsLabel={
        showValues
          ? "Selected connection values"
          : "Selected backing connection"
      }
      emptyMessage="No connections match your search."
      selectedId={selected?.attach.id}
      onSelect={setSelectedId}
      items={table.rows.map(({ attach, backingName, valueCount }) => ({
        id: attach.id,
        label: attach.name,
        description: [attach.service, backingName].filter(Boolean).join(" · "),
        status: showValues ? (
          <span className="shrink-0 text-xs text-muted-foreground">
            {attach.factLoadError
              ? "Unavailable"
              : `${valueCount} ${valueCount === 1 ? "value" : "values"}`}
          </span>
        ) : (
          <StatusBadge status={attach.status} className="shrink-0" />
        ),
      }))}
      toolbar={
        <ListToolbar
          label="connections"
          query={query}
          onQueryChange={setQuery}
          sort={table}
          fields={[
            { value: "name", label: "Name" },
            { value: "service", label: "Service" },
            { value: "backing", label: "Backing service" },
          ]}
        />
      }
      footer={
        table.pages > 1 && <TablePagination table={table} label="Connections" />
      }
    >
      {selected && (
        <div key={selected.attach.id} className="space-y-3">
          {children(selected.attach, selected.backingName)}
        </div>
      )}
    </InspectionWorkspace>
  );
}
