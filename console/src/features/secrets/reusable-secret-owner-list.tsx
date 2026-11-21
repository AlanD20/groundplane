import { ConfigurationTypeFilter } from "@/components/common/configuration-type-filter";
import { EmptyState } from "@/components/common/empty-state";
import { CompactReference } from "@/components/common/compact-reference";
import { HelpHint } from "@/components/common/resource-panel";
import { ResourceTable } from "@/components/common/resource-table";
import { RevealValue } from "@/components/common/reveal-value";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { formatTimestamp } from "@/lib/format-timestamp";
import type { ReusableSecret } from "@/lib/types";
import { Pencil, Trash2 } from "lucide-react";
import { useState } from "react";
import { ConfigurationValueEditor } from "./configuration-value-editor";

export function ReusableSecretOwnerList({
  secrets,
  loading,
  emptyTitle,
  emptyDescription,
  scopeLabel,
  onRemove,
  onRefresh,
  onReveal,
  editable = false,
}: {
  secrets: ReusableSecret[];
  loading: boolean;
  emptyTitle: string;
  emptyDescription?: string;
  scopeLabel: string;
  onRemove?: (
    secret: ReusableSecret,
    after: string[],
  ) => Promise<{ task_id: string }>;
  onRefresh?: () => Promise<void>;
  onReveal: (secret: ReusableSecret) => Promise<string>;
  editable?: boolean;
}) {
  const [editing, setEditing] = useState<ReusableSecret | null>(null);
  const [confirmation, setConfirmation] = useState<ReusableSecret | null>(null);
  const [query, setQuery] = useState("");
  const [kind, setKind] = useState("all");
  const matches = secrets.filter(
    (secret) =>
      (kind === "all" ||
        (secret.kind === "file"
          ? "file"
          : secret.sensitive
            ? "secret"
            : "variable") === kind) &&
      `${secret.key} ${secret.kind} ${secret.ref} ${scopeLabel}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
  );
  const table = useTableView(
    matches,
    {
      key: (secret) => secret.key,
      kind: (secret) => secret.kind,
      updated: (secret) => Date.parse(secret.updatedAt) || 0,
    },
    "key",
    "asc",
    `${scopeLabel}/${query}/${kind}`,
  );
  return (
    <>
      {loading && (
        <p role="status" className="py-3 text-xs text-muted-foreground">
          Loading configuration…
        </p>
      )}
      {!loading && secrets.length === 0 && (
        <EmptyState title={emptyTitle} description={emptyDescription} />
      )}
      {secrets.length > 0 && (
        <div className="space-y-4">
          <CollectionToolbar
            query={query}
            onQueryChange={setQuery}
            label="configuration"
          >
            <ConfigurationTypeFilter value={kind} onChange={setKind} />
          </CollectionToolbar>
          <ResourceTable>
            <Table className="min-w-[46rem]">
              <TableHeader>
                <TableRow>
                  <TableSortHead sort={table} field="key">
                    Key
                  </TableSortHead>
                  <TableHead>Storage</TableHead>
                  <TableSortHead
                    sort={table}
                    field="updated"
                    className="hidden sm:table-cell"
                  >
                    Updated
                  </TableSortHead>
                  <TableHead>
                    <span className="inline-flex items-center gap-1">
                      Value
                      <HelpHint label="About protected values">
                        Secret values are hidden until revealed. Encryption is a
                        separate storage choice shown for each value. Active
                        consumers and recoverable Tasks can prevent deletion.
                      </HelpHint>
                    </span>
                  </TableHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {table.rows.map((secret) => (
                  <TableRow key={secret.id}>
                    <TableCell className="min-w-48 max-w-xs">
                      <span className="block font-mono text-xs font-medium [overflow-wrap:anywhere]">
                        {secret.key}
                      </span>
                      <div className="mt-1 flex flex-wrap items-center gap-1">
                        <Badge variant="outline" className="font-normal">
                          {secret.kind === "file"
                            ? secret.sensitive
                              ? "Secret file"
                              : "File"
                            : secret.sensitive
                              ? "Secret"
                              : "Variable"}
                        </Badge>
                        <Badge
                          variant="outline"
                          className="font-normal text-muted-foreground"
                        >
                          {scopeLabel}
                        </Badge>
                      </div>
                      <div className="mt-1">
                        <CompactReference
                          value={secret.ref}
                          label="configuration reference"
                          short="Reference"
                          hideLabel
                        />
                      </div>
                    </TableCell>
                    <TableCell>
                      <Badge
                        variant={
                          secret.encryption === "age" ? "success" : "outline"
                        }
                      >
                        {secret.encryption === "age"
                          ? "Encrypted"
                          : "Not encrypted"}
                      </Badge>
                    </TableCell>
                    <TableCell className="hidden whitespace-nowrap text-xs text-muted-foreground sm:table-cell">
                      {formatTimestamp(secret.updatedAt, "Not reported")}
                    </TableCell>
                    <TableCell className="w-60 min-w-52">
                      <RevealValue
                        loadValue={() => onReveal(secret)}
                        label={secret.key}
                        sensitive={secret.sensitive}
                        file={secret.kind === "file"}
                      />
                    </TableCell>
                    <TableCell className="w-px whitespace-nowrap">
                      <div className="flex items-center justify-end gap-1">
                        {editable && (
                          <Button
                            variant="outline"
                            size="icon-sm"
                            onClick={() => setEditing(secret)}
                            aria-label={`Edit ${secret.key}`}
                            title={`Edit ${secret.key}`}
                          >
                            <Pencil className="size-3.5" />
                          </Button>
                        )}
                        {onRemove && (
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            className="text-muted-foreground hover:text-destructive"
                            onClick={() => setConfirmation(secret)}
                            aria-label={`Delete ${secret.key}`}
                            title={`Delete ${secret.key}`}
                          >
                            <Trash2 className="size-3.5" />
                          </Button>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            {matches.length === 0 && (
              <p className="p-6 text-center text-xs text-muted-foreground">
                No variables or files match your filters.
              </p>
            )}
          </ResourceTable>
          <TablePagination table={table} label="Configuration values" />
        </div>
      )}
      {editing && (
        <ConfigurationValueEditor
          key={editing.id}
          secret={editing}
          onClose={() => setEditing(null)}
          onSaved={async () => {
            await onRefresh?.();
          }}
        />
      )}
      {confirmation && onRemove && (
        <TaskRunnerDialog
          runAfter
          open
          onOpenChange={(open) => {
            if (!open) setConfirmation(null);
          }}
          title={`Delete ${confirmation.key}?`}
          description="Delete this configuration value. Active consumers and recoverable Tasks can prevent deletion."
          type="remove"
          target={confirmation.id}
          workspace={confirmation.scope === "platform" ? "Platform" : "Project"}
          startLabel="Delete configuration value"
          destructive
          onDispatch={async (after) =>
            (await onRemove(confirmation, after)).task_id
          }
          onCommit={() => {
            void onRefresh?.().catch(() => undefined);
          }}
        />
      )}
    </>
  );
}
