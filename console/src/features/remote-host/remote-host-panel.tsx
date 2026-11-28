import { useId, useRef, useState, type FormEvent } from "react";
import { ChevronLeft, ChevronRight, Download, Plus } from "lucide-react";
import { CreationButton } from "@/components/common/creation-button";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ResourceTable } from "@/components/common/resource-table";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Textarea } from "@/components/ui/textarea";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { useVisibleRefresh } from "@/lib/use-visible-refresh";
import { newULID } from "@/lib/utils";
import { RemoteImageFetch } from "./remote-image-fetch";
import {
  createRemoteHost,
  listRemoteHosts,
  readRemoteHost,
  revokeRemoteEnrollment,
  stageRemoteEnrollment,
  type RemoteHost,
  type RemoteHostEnrollment,
  type RemoteHostPage,
} from "./remote-host-api";

const stateLabels = {
  unenrolled: "Unenrolled",
  enrolling: "Enrollment pending",
  enrolled: "Enrolled",
};

const failureClass =
  "rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive";
const formatDate = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});
const csrPattern =
  /^-----BEGIN CERTIFICATE REQUEST-----\r?\n(?:[A-Za-z0-9+/=]+\r?\n)+-----END CERTIFICATE REQUEST-----$/;

type Edit =
  { kind: "register" } | { kind: "stage" | "revoke"; host: RemoteHost };

function explainError(cause: unknown) {
  if (cause instanceof ControllerRequestError && cause.status === 404)
    return "The resource was not found. Check whether this Controller supports Remote Host enrollment.";
  return cause instanceof Error
    ? cause.message
    : "The request could not be completed.";
}

function saveCertificate(enrollment: RemoteHostEnrollment) {
  const url = URL.createObjectURL(
    new Blob([enrollment.certificate], { type: "application/x-pem-file" }),
  );
  const download = document.createElement("a");
  download.href = url;
  download.download = `${enrollment.host.id}-agent-cert.pem`;
  download.click();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}

export function RemoteHostsPanel() {
  const inputId = useId();
  const [cursors, setCursors] = useState([""]);
  const cursor = cursors[cursors.length - 1];
  const [page, setPage] = useState<{
    cursor: string;
    result: RemoteHostPage;
  }>();
  const [selectedId, setSelectedId] = useState<string>();
  const [selected, setSelected] = useState<RemoteHost>();
  const [status, setStatus] = useState("");
  const [listError, setListError] = useState("");
  const [detailError, setDetailError] = useState("");
  const [mutationError, setMutationError] = useState("");
  const [retry, setRetry] = useState(0);
  const [edit, setEdit] = useState<Edit>();
  const [input, setInput] = useState("");
  const [certificate, setCertificate] = useState<RemoteHostEnrollment>();
  const [working, setWorking] = useState(false);
  const busy = useRef(false);
  const intent = useRef<{ fingerprint: string; key: string }>(undefined);
  const currentPage = page?.cursor === cursor ? page.result : undefined;
  const host = selected?.id === selectedId ? selected : undefined;
  const nextCursor = currentPage?.next_cursor;

  useVisibleRefresh(
    async (signal) => {
      try {
        const result = await listRemoteHosts(cursor, signal);
        if (signal.aborted) return;
        setPage({ cursor, result });
        setListError("");
      } catch (cause) {
        if (!signal.aborted) setListError(explainError(cause));
      }
    },
    { key: `${cursor}:${retry}` },
  );

  useVisibleRefresh(
    async (signal) => {
      if (!selectedId) return;
      try {
        const result = await readRemoteHost(selectedId, signal);
        if (signal.aborted) return;
        setSelected(result);
        setDetailError("");
      } catch (cause) {
        if (!signal.aborted) setDetailError(explainError(cause));
      }
    },
    { enabled: Boolean(selectedId), key: `${selectedId ?? ""}:${retry}` },
  );

  function startEdit(next?: Edit) {
    setEdit(next);
    setInput("");
    setMutationError("");
    setStatus("");
  }

  function select(id?: string) {
    startEdit();
    setDetailError("");
    setSelectedId(id);
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!edit || busy.current) return;
    if ("host" in edit && !Number.isSafeInteger(edit.host.generation)) {
      setMutationError(
        "The enrollment generation is outside the Console's supported range.",
      );
      return;
    }
    const value = input.trim();
    if (edit.kind === "stage" && !csrPattern.test(value)) {
      setMutationError(
        "Paste one PEM certificate signing request from agent.csr.pem.",
      );
      return;
    }
    if (edit.kind === "register" && !value) return;
    const fingerprint = JSON.stringify([
      edit.kind,
      "host" in edit ? edit.host.id : "",
      "host" in edit ? edit.host.generation : 0,
      value,
    ]);
    if (intent.current?.fingerprint !== fingerprint)
      intent.current = { fingerprint, key: newULID() };
    const { key } = intent.current;
    busy.current = true;
    setWorking(true);
    setMutationError("");
    try {
      if (edit.kind === "register") {
        const registered = await createRemoteHost(value, key);
        setSelectedId(registered.id);
        setSelected(registered);
        setStatus(`${registered.name} registered.`);
      } else if (edit.kind === "stage") {
        const enrollment = await stageRemoteEnrollment(edit.host, value, key);
        setSelected(enrollment.host);
        setCertificate(enrollment);
        setStatus(
          "Certificate issued. Install it on the Agent host to complete enrollment.",
        );
      } else {
        await revokeRemoteEnrollment(edit.host, key);
        setSelected(undefined);
        if (certificate?.host.id === edit.host.id) setCertificate(undefined);
        setStatus(
          `Agent enrollment generation ${edit.host.generation} revoked.`,
        );
      }
      intent.current = undefined;
      setEdit(undefined);
      setInput("");
      setRetry((value) => value + 1);
    } catch (cause) {
      setMutationError(explainError(cause));
    } finally {
      busy.current = false;
      setWorking(false);
    }
  }

  const enrollment =
    certificate &&
    host &&
    host.state !== "unenrolled" &&
    certificate.host.id === host.id &&
    certificate.host.generation === host.generation
      ? certificate
      : undefined;
  return (
    <ResourcePanel
      title="Remote Hosts"
      actions={
        <CreationButton
          variant="outline"
          size="sm"
          active={edit?.kind === "register"}
          disabled={!currentPage || Boolean(listError) || working}
          onOpenChange={(open) =>
            startEdit(open ? { kind: "register" } : undefined)
          }
        >
          <Plus aria-hidden className="size-4" /> Register Host
        </CreationButton>
      }
    >
      {listError && (
        <div role="alert" className={failureClass}>
          <p>{listError}</p>
          <Button
            type="button"
            variant="link"
            onClick={() => setRetry((value) => value + 1)}
          >
            Retry
          </Button>
        </div>
      )}
      {!currentPage && !listError && (
        <p role="status" className="text-sm text-muted-foreground">
          Loading Remote Hosts…
        </p>
      )}
      {currentPage && !(currentPage.items?.length ?? 0) && (
        <p className="text-sm text-muted-foreground">
          No Remote Hosts registered.
        </p>
      )}
      {Boolean(currentPage?.items?.length) && (
        <ResourceTable>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Host</TableHead>
                <TableHead>Enrollment</TableHead>
                <TableHead>Generation</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {currentPage?.items?.map((row) => (
                <TableRow
                  key={row.id}
                  className={selectedId === row.id ? "bg-muted" : undefined}
                >
                  <TableCell>
                    <Button
                      type="button"
                      variant="link"
                      className="h-auto p-0"
                      aria-expanded={selectedId === row.id}
                      disabled={working}
                      onClick={() =>
                        select(selectedId === row.id ? undefined : row.id)
                      }
                    >
                      {row.name}
                    </Button>
                  </TableCell>
                  <TableCell>{stateLabels[row.state]}</TableCell>
                  <TableCell className="tabular-nums">
                    {row.generation}
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </ResourceTable>
      )}
      {(cursors.length > 1 || nextCursor) && (
        <nav
          aria-label="Remote Host pages"
          className="flex items-center gap-2 text-sm text-muted-foreground"
        >
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label="Previous page"
            disabled={working || cursors.length < 2}
            onClick={() => setCursors((value) => value.slice(0, -1))}
          >
            <ChevronLeft aria-hidden />
          </Button>
          <span>Page {cursors.length}</span>
          <Button
            type="button"
            variant="outline"
            size="icon"
            aria-label="Next page"
            disabled={working || !nextCursor}
            onClick={() => {
              if (nextCursor) setCursors((value) => [...value, nextCursor]);
            }}
          >
            <ChevronRight aria-hidden />
          </Button>
        </nav>
      )}
      {selectedId && !host && !detailError && (
        <p role="status" className="text-sm text-muted-foreground">
          Loading Host enrollment…
        </p>
      )}
      {detailError && (
        <p role="alert" className={failureClass}>
          {detailError}
        </p>
      )}
      {host && (
        <section
          aria-label={`${host.name} enrollment`}
          className="space-y-3 rounded-lg border border-border p-4"
        >
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h3 className="font-medium">{host.name}</h3>
            <div className="flex flex-wrap gap-2">
              <CreationButton
                variant="outline"
                size="sm"
                active={edit?.kind === "stage"}
                disabled={working || Boolean(detailError)}
                onOpenChange={(open) =>
                  startEdit(open ? { kind: "stage", host } : undefined)
                }
              >
                {host.state === "enrolled"
                  ? "Replace certificate"
                  : "Stage certificate"}
              </CreationButton>
              {host.state !== "unenrolled" && (
                <CreationButton
                  variant="outline"
                  size="sm"
                  active={edit?.kind === "revoke"}
                  disabled={working || Boolean(detailError)}
                  onOpenChange={(open) =>
                    startEdit(open ? { kind: "revoke", host } : undefined)
                  }
                >
                  Revoke enrollment
                </CreationButton>
              )}
            </div>
          </div>
          <dl className="grid gap-3 text-sm sm:grid-cols-2">
            <div>
              <dt className="text-muted-foreground">Host ID</dt>
              <dd className="break-all font-mono">{host.id}</dd>
            </div>
            <div>
              <dt className="text-muted-foreground">Enrollment</dt>
              <dd>
                {stateLabels[host.state]} · Generation {host.generation}
              </dd>
            </div>
            {host.agent_id && (
              <div>
                <dt className="text-muted-foreground">Agent ID</dt>
                <dd className="break-all font-mono">{host.agent_id}</dd>
              </div>
            )}
            {host.credential_expiry && (
              <div>
                <dt className="text-muted-foreground">
                  {host.state === "enrolling"
                    ? "Confirm by"
                    : "Certificate expires"}
                </dt>
                <dd>{formatDate.format(new Date(host.credential_expiry))}</dd>
              </div>
            )}
          </dl>
          {host.state === "enrolled" && (
            <RemoteImageFetch
              key={host.id}
              hostId={host.id}
              disabled={working || Boolean(detailError)}
            />
          )}
        </section>
      )}
      {edit && (
        <form
          className="space-y-3 rounded-lg border border-border p-4"
          onSubmit={submit}
        >
          {edit.kind === "register" && (
            <div className="max-w-lg space-y-1.5">
              <Label htmlFor={inputId}>Host name</Label>
              <Input
                id={inputId}
                value={input}
                disabled={working}
                onChange={(event) => setInput(event.target.value)}
                maxLength={253}
                required
              />
            </div>
          )}
          {edit.kind === "stage" && (
            <>
              <p className="text-sm">
                Run{" "}
                <code className="break-words">
                  groundplane remote-host prepare-enrollment ./agent-enrollment
                </code>{" "}
                on {edit.host.name}. Keep the private key on that Host.
              </p>
              <div className="space-y-1.5">
                <Label htmlFor={inputId}>Certificate signing request</Label>
                <Textarea
                  id={inputId}
                  value={input}
                  disabled={working}
                  onChange={(event) => {
                    const value = event.target.value;
                    if (/-----BEGIN [A-Z ]*PRIVATE KEY-----/.test(value)) {
                      setInput("");
                      setMutationError(
                        "Keep the private key on the Agent. Paste agent.csr.pem here.",
                      );
                    } else {
                      setInput(value);
                      setMutationError("");
                    }
                  }}
                  spellCheck={false}
                  maxLength={24000}
                  rows={5}
                  required
                  className="font-mono"
                  placeholder="-----BEGIN CERTIFICATE REQUEST-----"
                />
              </div>
            </>
          )}
          {edit.kind === "revoke" && (
            <p className="text-sm">
              Revoke generation {edit.host.generation} for {edit.host.name}.
              This Agent will lose access to its Controller enrollment.
            </p>
          )}
          {mutationError && (
            <p role="alert" className={failureClass}>
              {mutationError}
            </p>
          )}
          <div className="flex flex-wrap justify-end gap-2">
            <Button
              type="button"
              variant="outline"
              disabled={working}
              onClick={() => startEdit()}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              variant={edit.kind === "revoke" ? "destructive" : "default"}
              disabled={working || (edit.kind !== "revoke" && !input.trim())}
            >
              {working
                ? "Submitting…"
                : edit.kind === "register"
                  ? "Register Host"
                  : edit.kind === "stage"
                    ? "Issue certificate"
                    : "Revoke enrollment"}
            </Button>
          </div>
        </form>
      )}
      {status && (
        <p role="status" className="text-sm">
          {status}
        </p>
      )}
      {enrollment && (
        <section
          aria-label="Issued Agent certificate"
          className="space-y-3 rounded-lg border border-border p-4"
        >
          <h3 className="font-medium">Agent certificate</h3>
          <p className="text-sm text-muted-foreground">
            Install this certificate alongside the private key on{" "}
            {enrollment.host.name}. Configure the Agent with the Host ID, Agent
            ID, generation, private Controller endpoint and trust certificates.
          </p>
          <p className="text-sm text-muted-foreground">
            Certificate expires{" "}
            {formatDate.format(new Date(enrollment.expires_at))}.
          </p>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => saveCertificate(enrollment)}
          >
            <Download aria-hidden className="size-4" /> Download certificate
          </Button>
        </section>
      )}
    </ResourcePanel>
  );
}
