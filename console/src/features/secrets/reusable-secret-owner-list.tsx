import { EmptyState } from "@/components/common/empty-state";
import { AdvancedDetails, HelpHint } from "@/components/common/resource-panel";
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
import { Trash2 } from "lucide-react";
import { useState } from "react";

export function ReusableSecretOwnerList({
  secrets,
  loading,
  emptyTitle,
  emptyDescription,
  scopeLabel,
  onRemove,
  onRefresh,
  onReveal,
}: {
  secrets: ReusableSecret[];
  loading: boolean;
  emptyTitle: string;
  emptyDescription?: string;
  scopeLabel: string;
  onRemove?: (secret: ReusableSecret) => Promise<{ task_id: string }>;
  onRefresh?: () => Promise<void>;
  onReveal: (secret: ReusableSecret) => Promise<string>;
}) {
  const [confirmation, setConfirmation] = useState<ReusableSecret | null>(null);
  const [query, setQuery] = useState("");
  const matches = secrets.filter((secret) =>
    `${secret.key} ${secret.kind}`
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
    `${scopeLabel}/${query}`,
  );
  return (
    <>
      {loading && (
        <p role="status" className="py-3 text-xs text-muted-foreground">
          Loading Secrets…
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
            label="Secrets"
          >
            <HelpHint label="About Secret values">
              Values are fetched only when explicitly revealed. Deletion remains
              protected when a resource or recoverable Task uses the Secret.
            </HelpHint>
          </CollectionToolbar>
          <ResourceTable>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableSortHead sort={table} field="key">
                    Secret
                  </TableSortHead>
                  <TableSortHead sort={table} field="kind">
                    Type
                  </TableSortHead>
                  <TableSortHead sort={table} field="updated">
                    Updated
                  </TableSortHead>
                  <TableHead className="text-right">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {table.rows.map((secret) => (
                  <TableRow key={secret.id}>
                    <TableCell className="font-mono font-medium">
                      {secret.key}
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">
                        {secret.kind === "file" ? "File" : "Variable"}
                      </Badge>
                    </TableCell>
                    <TableCell className="text-muted-foreground">
                      {formatTimestamp(secret.updatedAt, "Not reported")}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center justify-end gap-2">
                        <RevealValue
                          loadValue={() => onReveal(secret)}
                          label={secret.key}
                        />
                        {onRemove && (
                          <Button
                            variant="ghost"
                            size="icon-sm"
                            className="text-muted-foreground hover:text-destructive"
                            onClick={() => setConfirmation(secret)}
                            aria-label={`Delete ${secret.key}`}
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
                No Secrets match your search.
              </p>
            )}
          </ResourceTable>
          <TablePagination table={table} label="Secrets" />
          <AdvancedDetails title="Secret references">
            <div className="space-y-2">
              {table.rows.map((secret) => (
                <div
                  key={secret.id}
                  className="grid min-w-0 gap-1 sm:grid-cols-2"
                >
                  <span>{secret.key}</span>
                  <code className="break-all text-muted-foreground">
                    {secret.ref}
                  </code>
                </div>
              ))}
            </div>
          </AdvancedDetails>
        </div>
      )}
      {confirmation && onRemove && (
        <TaskRunnerDialog
          open
          onOpenChange={(open) => {
            if (!open) setConfirmation(null);
          }}
          title={`Delete ${confirmation.key}?`}
          description="Delete this reusable Secret. The Controller rejects deletion while it is still in use."
          type="remove"
          target={confirmation.id}
          workspace={confirmation.scope === "platform" ? "Platform" : "Project"}
          steps={[]}
          startLabel="Delete Secret"
          destructive
          onDispatch={async () => (await onRemove(confirmation)).task_id}
          onCommit={() => {
            void onRefresh?.().catch(() => undefined);
          }}
        />
      )}
    </>
  );
}
