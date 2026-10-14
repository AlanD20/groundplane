import { CreationButton } from "@/components/common/creation-button";
import { Fragment, useState } from "react";
import { Plus, RefreshCw, Pencil, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ResourceTable, ResourceRow } from "@/components/common/resource-table";
import { TableExpansion } from "@/components/common/table-expansion";
import { ResourceActionMenu } from "@/components/common/resource-action-menu";
import {
  CollectionToolbar,
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskLink } from "@/components/common/task-link";
import { useRequiredParams } from "@/lib/router";
import type { Environment } from "@/lib/types";
import { DNSEditor } from "./dns-editor";
import { useEnvironmentDNS } from "./use-environment-dns";
import type { DNSRecord } from "./api";

const states = {
  pending: { label: "Waiting for CoreDNS", tone: "pending" },
  applied: { label: "Applied", tone: "completed" },
  failed: { label: "CoreDNS update failed", tone: "failed" },
  resolver_disabled: { label: "CoreDNS is disabled", tone: "stopped" },
  waiting_for_service: { label: "Waiting for Service", tone: "pending" },
};

export function EnvironmentDNSPanel({ env }: { env: Environment }) {
  const params = useRequiredParams("tenant", "project");
  const dns = useEnvironmentDNS(env.id);
  const [query, setQuery] = useState("");
  const [editor, setEditor] = useState<{
    revision: string;
    record?: DNSRecord;
  } | null>(null);
  const [removal, setRemoval] = useState<{
    revision: string;
    record: DNSRecord;
  } | null>(null);
  const [removeError, setRemoveError] = useState<string>();
  const [notice, setNotice] = useState<string>();
  const rows = (dns.view?.records ?? []).filter((record) =>
    `${record.hostname} ${record.service ?? ""} ${record.address ?? ""}`
      .toLowerCase()
      .includes(query.trim().toLowerCase()),
  );
  const table = useTableView(
    rows,
    {
      name: (record) => record.hostname,
      target: (record) => record.address ?? record.service ?? "",
      state: (record) => states[record.state].label,
    },
    "name",
    "asc",
    `${env.id}/${query}`,
  );
  const edit = (record: DNSRecord) => {
    if (dns.view && !dns.saving) {
      setRemoval(null);
      setEditor(
        editor?.record?.id === record.id
          ? null
          : { revision: dns.view.revision, record },
      );
    }
  };
  async function remove() {
    if (!removal) return;
    setRemoveError(undefined);
    try {
      await dns.save({
        action: "remove",
        recordId: removal.record.id,
        revision: removal.revision,
      });
      setRemoval(null);
      setNotice(
        "Removal saved. CoreDNS must withdraw the answer before its hostname is released.",
      );
    } catch (cause) {
      setRemoveError(
        cause instanceof Error ? cause.message : "Unable to remove DNS record.",
      );
    }
  }
  const visibleRows =
    editor?.record &&
    !table.rows.some((record) => record.id === editor.record?.id)
      ? [editor.record, ...table.rows]
      : table.rows;
  const editorForm = editor && (
    <DNSEditor
      key={editor.record?.id ?? "new"}
      env={env}
      tenant={params.tenant}
      project={params.project}
      record={editor.record}
      saving={dns.saving}
      onClose={() => setEditor(null)}
      onSave={async (body) => {
        await dns.save(
          editor.record
            ? {
                action: "edit",
                recordId: editor.record.id,
                revision: editor.revision,
                body,
              }
            : { action: "create", revision: editor.revision, body },
        );
        setNotice("DNS record saved. Resolver activation is shown below.");
      }}
    />
  );
  return (
    <section className="space-y-4" aria-label="Environment DNS">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold">DNS records</h2>
          <p className="text-sm text-muted-foreground">
            Environment-owned names on the shared CoreDNS resolver.
          </p>
        </div>
        <CreationButton
          active={!!editor}
          disabled={!dns.view || dns.saving}
          onClick={() => {
            if (dns.view) {
              setRemoval(null);
              setEditor({ revision: dns.view.revision });
            }
          }}
        >
          <Plus />
          Add record
        </CreationButton>
      </div>
      {dns.loading && (
        <p role="status" className="text-sm text-muted-foreground">
          Loading DNS records…
        </p>
      )}
      {dns.error && (
        <p role="alert" className="text-sm text-destructive">
          {dns.error}
        </p>
      )}
      {notice && (
        <p role="status" className="text-sm text-muted-foreground">
          {notice}
        </p>
      )}
      {dns.view && (
        <div className="flex flex-wrap items-center gap-3">
          <StatusBadge
            status={states[dns.view.state].tone}
            label={states[dns.view.state].label}
          />
          {dns.view.task_id && (
            <TaskLink taskId={dns.view.task_id}>View resolver Task</TaskLink>
          )}
        </div>
      )}
      {editor && !editor.record && editorForm}
      {removal && (
        <Dialog
          open
          onOpenChange={(open) => {
            if (!open && !dns.saving) setRemoval(null);
          }}
        >
          <DialogContent>
            <DialogHeader>
              <DialogTitle className="break-all">
                Remove {removal.record.hostname}?
              </DialogTitle>
              <DialogDescription>
                The shared resolver will withdraw this name. The target Service
                is unchanged.
              </DialogDescription>
            </DialogHeader>
            {removeError && (
              <p role="alert" className="text-sm text-destructive">
                {removeError}
              </p>
            )}
            <DialogFooter>
              <Button
                disabled={dns.saving}
                variant="outline"
                onClick={() => setRemoval(null)}
              >
                Cancel
              </Button>
              <Button
                disabled={dns.saving}
                variant="destructive"
                onClick={() => void remove()}
              >
                {dns.saving ? "Saving…" : "Remove record"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      )}
      <CollectionToolbar
        query={query}
        onQueryChange={setQuery}
        label="DNS records"
      >
        <Button
          variant="outline"
          aria-label="Refresh DNS status"
          onClick={() => void dns.refresh()}
        >
          <RefreshCw />
        </Button>
      </CollectionToolbar>
      <ResourceTable>
        <Table>
          <TableHeader>
            <TableRow>
              <TableSortHead sort={table} field="name">
                Hostname
              </TableSortHead>
              <TableSortHead sort={table} field="target">
                Target
              </TableSortHead>
              <TableSortHead sort={table} field="state">
                Status
              </TableSortHead>
              <TableHead>
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {visibleRows.map((record) => (
              <Fragment key={record.id}>
                <ResourceRow onOpen={() => edit(record)}>
                  <TableCell>
                    <Button
                      variant="link"
                      className="h-auto whitespace-normal break-all p-0 text-left"
                      disabled={dns.saving}
                      aria-expanded={editor?.record?.id === record.id}
                      onClick={() => edit(record)}
                    >
                      {record.hostname}
                    </Button>
                  </TableCell>
                  <TableCell>
                    <div className="break-all text-sm">
                      {record.address || record.service}
                    </div>
                    {record.service && (
                      <p className="text-xs text-muted-foreground">
                        {record.zone}
                        {record.resolved_address
                          ? ` / ${record.resolved_address}`
                          : ""}
                      </p>
                    )}
                  </TableCell>
                  <TableCell>
                    <div className="space-y-1">
                      <StatusBadge
                        status={states[record.state].tone}
                        label={states[record.state].label}
                      />
                      {record.task_id && (
                        <div>
                          <TaskLink taskId={record.task_id}>View Task</TaskLink>
                        </div>
                      )}
                    </div>
                  </TableCell>
                  <TableCell>
                    <ResourceActionMenu
                      label={`Actions for ${record.hostname}`}
                      actions={[
                        {
                          label: "Edit",
                          icon: <Pencil />,
                          disabled: dns.saving,
                          onSelect: () => edit(record),
                        },
                        {
                          label: "Remove",
                          icon: <Trash2 />,
                          destructive: true,
                          disabled: dns.saving,
                          onSelect: () => {
                            if (dns.view) {
                              setEditor(null);
                              setRemoveError(undefined);
                              setRemoval({
                                revision: dns.view.revision,
                                record,
                              });
                            }
                          },
                        },
                      ]}
                    />
                  </TableCell>
                </ResourceRow>
                <TableExpansion columns={4}>
                  {editor?.record?.id === record.id ? editorForm : null}
                </TableExpansion>
              </Fragment>
            ))}
            {!visibleRows.length && (
              <TableRow>
                <TableCell
                  colSpan={4}
                  className="py-8 text-center text-muted-foreground"
                >
                  {dns.loading
                    ? "Loading…"
                    : query
                      ? "No matching DNS records."
                      : "No DNS records in this Environment."}
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </ResourceTable>
      <TablePagination table={table} label="DNS records" />
      {!!dns.view?.withdrawals.length && (
        <div className="rounded-lg border border-border p-3 text-sm">
          <p className="font-medium">Awaiting withdrawal</p>
          <ul className="mt-2 space-y-1">
            {dns.view.withdrawals.map((item) => (
              <li
                key={`${item.record_id}/${item.hostname}`}
                className="break-all font-mono text-xs"
              >
                {item.hostname}
              </li>
            ))}
          </ul>
        </div>
      )}
    </section>
  );
}
