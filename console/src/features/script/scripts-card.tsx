import { ResourceForm } from "@/components/common/resource-form";
import { workspaceSectionClassName } from "@/components/common/workspace-section";
import {
  ListToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Badge } from "@/components/ui/badge";
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
  const [runError, setRunError] = useState<string>();
  const [runningRequest, setRunningRequest] = useState<string>();
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

  return (
    <>
      <Card>
        <CardHeader className="flex-row items-center justify-between">
          <CardTitle className="flex items-center gap-2">
            <Terminal className="size-4 text-muted-foreground" /> Scripts
          </CardTitle>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              setEditing(null);
              setAddOpen(true);
            }}
          >
            <Plus className="size-3.5" /> Add Script
          </Button>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <ListToolbar
            label="Scripts"
            query={query}
            onQueryChange={setQuery}
            sort={table}
            fields={[
              { value: "name", label: "Name" },
              { value: "order", label: "Order" },
              { value: "service", label: "Service" },
            ]}
          />
          {runError && (
            <p role="alert" className="text-xs text-destructive">
              {runError}
            </p>
          )}
          {table.rows.map((script) => (
            <div
              key={script.id}
              className={workspaceSectionClassName(
                false,
                "flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between",
              )}
            >
              <div className="flex min-w-0 flex-col">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="break-all font-mono text-sm">
                    {script.slug}
                  </span>
                  <Badge
                    variant={script.when === "manual" ? "default" : "primary"}
                  >
                    {script.when}
                  </Badge>
                  <span className="text-xs text-muted-foreground">
                    order {script.order} · {script.execution.mode}
                  </span>
                </div>
                <span className="truncate font-mono text-xs text-muted-foreground">
                  {script.body.split("\n")[0]}
                  {script.body.split("\n").length > 1
                    ? ` · +${script.body.split("\n").length - 1} lines`
                    : ""}
                </span>
                {!servingServices.has(script.service) && (
                  <span className="text-xs text-muted-foreground">
                    Deploy {script.service} before running this Script manually.
                    {script.when === "pre-deploy" &&
                      " This hook runs automatically during Deploy."}
                  </span>
                )}
              </div>
              <div className="flex shrink-0 flex-wrap items-center gap-2">
                <span className="font-mono text-xs text-muted-foreground">
                  → {script.service}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={
                    !!runningRequest || !servingServices.has(script.service)
                  }
                  onClick={async () => {
                    setRunError(undefined);
                    setRunningRequest(script.id);
                    try {
                      await store.runScript(script.id);
                    } catch (error) {
                      setRunError(
                        error instanceof Error
                          ? error.message
                          : "Unable to run Script",
                      );
                    } finally {
                      setRunningRequest(undefined);
                    }
                  }}
                >
                  <Terminal className="size-3.5" />{" "}
                  {runningRequest === script.id ? "Submitting…" : "Run"}
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  title="Edit script"
                  aria-label={`Edit script ${script.slug}`}
                  onClick={() => {
                    setAddOpen(false);
                    setEditing(script);
                  }}
                >
                  <Pencil className="size-3.5" />
                </Button>
                <Button
                  variant="ghost"
                  size="icon-xs"
                  className="text-muted-foreground hover:text-destructive"
                  title="Remove script"
                  aria-label={`Remove script ${script.slug}`}
                  onClick={() => setRemoving(script)}
                >
                  <Trash2 className="size-3.5" />
                </Button>
              </div>
            </div>
          ))}
          {env.scripts.length === 0 && (
            <p className="text-xs text-muted-foreground">
              No scripts — hooks run automatically on deploy/rollback.
            </p>
          )}
          {env.scripts.length > 0 && !table.total && (
            <p className="p-6 text-center text-xs text-muted-foreground">
              No Scripts match your search.
            </p>
          )}
          <TablePagination table={table} label="Scripts" />
        </CardContent>
      </Card>
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
      <TaskRunnerDialog
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
        steps={[
          { label: "Validate the script record", state: "pending" },
          { label: "Remove the automatic hook registration", state: "pending" },
          { label: "Remove the script", state: "pending" },
        ]}
        onDispatch={async () => {
          if (!removing) throw new Error("No Script selected for removal");
          return store.removeScript(env.id, removing.id);
        }}
      />
    </>
  );
}
