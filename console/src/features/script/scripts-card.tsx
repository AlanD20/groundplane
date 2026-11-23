import { InspectionWorkspace } from "@/components/common/inspection-workspace";
import { EmptyState } from "@/components/common/empty-state";
import { CreationButton } from "@/components/common/creation-button";
import { ResourceForm } from "@/components/common/resource-form";
import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Badge } from "@/components/ui/badge";
import { CodeEditor } from "@/components/ui/code-editor";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, Script } from "@/lib/types";
import { Pencil, Plus, Terminal, Trash2 } from "lucide-react";
import { useState } from "react";
import { ScriptEditor } from "./script-editor";

export function ScriptsCard({ env }: { env: Environment }) {
  const store = useStore();
  const params = useRequiredParams("tenant");
  const [addOpen, setAddOpen] = useState(false);
  const [editing, setEditing] = useState<Script | null>(null);
  const [removing, setRemoving] = useState<Script | null>(null);
  const [query, setQuery] = useState("");
  const [selectedId, setSelectedId] = useState<string>();
  const [running, setRunning] = useState<Script | null>(null);
  const table = useTableView(
    env.scripts.filter((script) =>
      `${script.slug} ${script.service} ${script.when}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
    ),
    {
      name: (script) => script.slug,
      order: (script) => script.order,
      service: (script) => script.service,
    },
    "order",
    "asc",
    `${env.id}/${query}`,
  );
  function closeEditor() {
    setAddOpen(false);
    setEditing(null);
  }
  const servingServices = new Set(
    env.deploys
      .filter((release) => release.status === "active")
      .map((release) => release.service),
  );
  const selected =
    editing ??
    table.rows.find((script) => script.id === selectedId) ??
    table.rows[0];
  const visibleScripts =
    editing && !table.rows.some((script) => script.id === editing.id)
      ? [editing, ...table.rows]
      : table.rows;
  const runReview = (
    <TaskRunnerDialog
      runAfter
      open={!!selected && running?.id === selected.id}
      onOpenChange={(open) => {
        if (!open) setRunning(null);
      }}
      variant="inline"
      title={`Run script · ${running?.slug ?? ""}`}
      description="Run this Script once against the serving deployment selected after prerequisite Tasks settle."
      type="script"
      target={running?.id ?? env.id}
      workspace={params.tenant}
      startLabel="Run Script"
      startDisabled={(after) =>
        !running ||
        (!servingServices.has(running.service) && after.length === 0)
      }
      onDispatch={async (after) => {
        if (!running) throw new Error("No Script selected");
        return store.runScript(running.id, after);
      }}
    />
  );

  const editor = (
    <ResourceForm
      open={addOpen || !!editing}
      onOpenChange={(next) => {
        if (!next) closeEditor();
      }}
    >
      {(addOpen || editing) && (
        <ScriptEditor
          key={editing?.id ?? "new"}
          env={env}
          script={editing}
          onClose={closeEditor}
          onSave={async (input) => {
            if (editing) {
              const { service: _service, ...patch } = input;
              await store.updateScript(env.id, editing.id, patch);
            } else {
              await store.addScript(env.id, input);
            }
          }}
        />
      )}
    </ResourceForm>
  );
  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Terminal className="size-4 text-muted-foreground" /> Scripts
          </CardTitle>
          <CreationButton
            active={addOpen}
            disabled={!!editing}
            variant="outline"
            size="sm"
            onOpenChange={(open) => {
              setEditing(null);
              setAddOpen(open);
            }}
          >
            <Plus className="size-3.5" /> Add Script
          </CreationButton>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          {addOpen && editor}
          {env.scripts.length ? (
            <InspectionWorkspace
              title="Script list"
              selectionLabel="Select script"
              detailsLabel="Selected script"
              emptyMessage="No scripts match your search."
              selectedId={selected?.id}
              onSelect={setSelectedId}
              selectionDisabled={!!editing}
              items={visibleScripts.map((script) => ({
                id: script.id,
                label: script.slug,
                description: script.service,
                status: <Badge variant="outline">{script.when}</Badge>,
              }))}
              toolbar={
                <ListToolbar
                  label="scripts"
                  query={query}
                  onQueryChange={setQuery}
                  sort={table}
                  fields={[
                    { value: "name", label: "Name" },
                    { value: "order", label: "Order" },
                    { value: "service", label: "Service" },
                  ]}
                />
              }
              footer={
                table.pages > 1 && (
                  <TablePagination table={table} label="Scripts" />
                )
              }
            >
              {selected && (
                <div className="space-y-4">
                  <header className="flex flex-wrap items-start justify-between gap-3 border-b border-border pb-3">
                    <div>
                      <h3 className="text-sm font-semibold [overflow-wrap:anywhere]">
                        {selected.slug}
                      </h3>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {selected.service} · {selected.when} · Order{" "}
                        {selected.order}
                      </p>
                      <p className="mt-1 text-xs text-muted-foreground">
                        {selected.execution.mode === "explicit"
                          ? "Separate container"
                          : "Service configuration"}
                      </p>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        aria-expanded={running?.id === selected.id}
                        onClick={() =>
                          setRunning(
                            running?.id === selected.id ? null : selected,
                          )
                        }
                      >
                        <Terminal className="size-3.5" /> Run
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        aria-expanded={editing?.id === selected.id}
                        onClick={() => {
                          setAddOpen(false);
                          setEditing(
                            editing?.id === selected.id ? null : selected,
                          );
                        }}
                      >
                        <Pencil className="size-3.5" /> Edit
                      </Button>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setRemoving(selected)}
                      >
                        <Trash2 className="size-3.5" /> Remove script
                      </Button>
                    </div>
                  </header>
                  {editing?.id === selected.id && editor}
                  {runReview}
                  {!servingServices.has(selected.service) && (
                    <p className="text-xs text-muted-foreground">
                      Deploy {selected.service} first, or choose its pending
                      Deploy Task as a prerequisite for a manual run.
                      {selected.when === "pre-deploy" &&
                        " This hook runs automatically during Deploy."}
                    </p>
                  )}
                  {editing?.id !== selected.id && (
                    <CodeEditor
                      id={`script-preview-${selected.id}`}
                      label="Script source"
                      language="shell"
                      value={selected.body}
                      className="[--editor-toolbar-top:-1rem]"
                      readOnly
                      scrollWithParent
                    />
                  )}
                </div>
              )}
            </InspectionWorkspace>
          ) : (
            <EmptyState
              embedded
              title="No scripts yet"
              description="Add a Script to run manually or as a deployment hook."
            />
          )}
        </CardContent>
      </Card>
      <TaskRunnerDialog
        runAfter
        open={!!removing}
        onOpenChange={(next) => !next && setRemoving(null)}
        title={`Remove script · ${removing?.slug ?? ""}`}
        description="Removes this script and its deploy or rollback hook from the environment."
        type="destroy"
        target={removing?.id ?? env.id}
        workspace={params.tenant}
        destructive
        confirmText={removing?.slug ?? ""}
        startLabel="Remove script"
        onDispatch={async (after) => {
          if (!removing) throw new Error("No Script selected for removal");
          return store.removeScript(env.id, removing.id, after);
        }}
      />
    </>
  );
}
