import { useEffect, useRef, useState } from "react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { InlineEditorRegion } from "@/components/common/inline-editor-region";
import { CompactReference } from "@/components/common/compact-reference";
import { Button } from "@/components/ui/button";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useStore } from "@/lib/store";
import type { Runner } from "@/lib/types";
import {
  captureCLIGrant,
  pendingCLIGrant,
  readCLIGrant,
  releaseCLIGrant,
  submitCLIGrant,
  type GrantObservation,
  type GrantWrite,
} from "./cli-grant-api";
import { CLIGrantFields, operationLabels } from "./cli-grant-fields";

export function CLIGrantPanel({ runner }: { runner: Runner }) {
  const store = useStore();
  const pending = pendingCLIGrant(runner.id);
  const [observation, setObservation] = useState<GrantObservation>();
  const [reading, setReading] = useState(true);
  const [epoch, setEpoch] = useState(0);
  const [readError, setReadError] = useState("");
  const [writeError, setWriteError] = useState("");
  const [notice, setNotice] = useState("");
  const [revision, setRevision] = useState(pending?.revision ?? "");
  const [mode, setMode] = useState<"edit" | "revoke">();
  const [busy, setBusy] = useState(false);
  const inFlight = useRef(false);
  const choices = store.tenantProjects
    .filter(
      (project) =>
        project.tenantId === runner.tenantId &&
        !project.deletionTaskId &&
        (!runner.projectId || runner.projectId === project.id),
    )
    .flatMap((project) =>
      (project.environments ?? [])
        .filter(
          (environment) =>
            environment.provisioningState === "ready" &&
            !environment.deletionTaskId &&
            (!runner.environmentId || runner.environmentId === environment.id),
        )
        .map((environment) => ({
          value: environment.id,
          label: `${project.name} · ${environment.name}`,
          scope: {
            tenant_id: runner.tenantId,
            project_id: project.id,
            environment_id: environment.id,
          },
        })),
    );
  const grant = observation?.status;
  const scopeLabel =
    choices.find((choice) => choice.value === grant?.scope?.environment_id)
      ?.label ?? grant?.scope?.environment_id;
  const stale = observation?.revision !== revision;

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    let expires: ReturnType<typeof setTimeout>;
    async function load() {
      try {
        const next = await readCLIGrant(runner.id, signal);
        if (signal.aborted) return;
        setObservation(next);
        setReadError("");
        const renew = next.status.expires_at
          ? Date.parse(next.status.expires_at)
          : 0;
        if (next.status.active && renew > Date.now()) {
          expires = setTimeout(load, renew - Date.now() + 1_000);
        }
      } catch (cause) {
        if (!signal.aborted)
          setReadError(
            cause instanceof Error
              ? cause.message
              : "Could not load Runner CLI access.",
          );
      } finally {
        if (!signal.aborted) setReading(false);
      }
    }
    void load();
    return () => {
      controller.abort();
      clearTimeout(expires);
    };
  }, [epoch, runner.id]);

  function refresh() {
    setReading(true);
    setEpoch((current) => current + 1);
  }

  function toggle(next?: "edit" | "revoke") {
    if (inFlight.current || pending) return;
    setRevision(observation?.revision ?? "");
    setMode(next === mode ? undefined : next);
    setNotice("");
    setWriteError("");
  }

  async function send(body?: GrantWrite) {
    if (
      inFlight.current ||
      (!pending &&
        (stale ||
          reading ||
          readError ||
          !observation ||
          (body ? !grant?.available : !grant?.actions.length)))
    )
      return;
    inFlight.current = true;
    setBusy(true);
    setWriteError("");
    const intent = pending ?? captureCLIGrant(runner.id, revision, body);
    try {
      await submitCLIGrant(runner.id, intent);
      releaseCLIGrant(runner.id, intent);
      setNotice(
        intent.body ? "Runner CLI grant saved." : "Runner CLI grant revoked.",
      );
      setMode(undefined);
      refresh();
    } catch (cause) {
      const rejected =
        cause instanceof ControllerRequestError &&
        cause.status >= 400 &&
        cause.status < 500 &&
        cause.status !== 408 &&
        cause.status !== 429 &&
        cause.code !== "idempotency.in_progress";
      if (rejected) {
        releaseCLIGrant(runner.id, intent);
        if ([409, 412].includes(cause.status)) refresh();
      }
      setWriteError(
        rejected
          ? cause.message
          : "The Controller has yet to confirm the request. Retry with the original grant and revision.",
      );
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }

  return (
    <ResourcePanel
      title="Runner CLI access"
      actions={
        <>
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={reading || busy}
            onClick={refresh}
          >
            {reading ? "Checking…" : "Refresh"}
          </Button>
          {grant?.available && (
            <Button
              type="button"
              size="sm"
              variant="outline"
              aria-expanded={mode === "edit"}
              disabled={busy || reading || Boolean(readError || pending)}
              onClick={() => toggle("edit")}
            >
              {grant.actions.length ? "Edit grant" : "Grant CLI access"}
            </Button>
          )}
          {Boolean(grant?.actions.length) && (
            <Button
              type="button"
              size="sm"
              variant="outline"
              aria-expanded={mode === "revoke"}
              disabled={busy || reading || Boolean(readError || pending)}
              onClick={() => toggle("revoke")}
            >
              Revoke grant
            </Button>
          )}
        </>
      }
    >
      <p className="text-sm text-muted-foreground">
        A CLI grant allows specific Groundplane operations in one Environment
        until it expires.
      </p>
      {notice && (
        <p role="status" className="text-sm text-success">
          {notice}
        </p>
      )}
      {readError && (
        <p role="alert" className="text-sm text-destructive">
          {readError}
        </p>
      )}
      {grant && (
        <div className="space-y-2 rounded-lg border border-border p-3 text-sm">
          <p className="font-medium">
            {grant.active
              ? "Grant active"
              : grant.actions.length
                ? "Grant inactive"
                : "No CLI grant"}
          </p>
          {scopeLabel && (
            <p className="text-muted-foreground">Environment · {scopeLabel}</p>
          )}
          {grant.expires_at && (
            <p className="text-muted-foreground">
              Expiration ·{" "}
              {formatTimestamp(grant.expires_at, "Time unavailable")}
            </p>
          )}
          {grant.actions.length > 0 && (
            <ul className="divide-y divide-border">
              {grant.actions.map((action) => (
                <li
                  key={`${action.operation}/${action.target_id}`}
                  className="space-y-1 py-2"
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <span>{operationLabels[action.operation]}</span>
                    <CompactReference
                      label={
                        action.operation === "task.read" ? "Task" : "Service"
                      }
                      value={action.target_id}
                    />
                  </div>
                  {action.image && (
                    <CompactReference label="Image" value={action.image} />
                  )}
                  {action.task_ids?.map((id) => (
                    <CompactReference
                      key={id}
                      label="Prerequisite Task"
                      value={id}
                    />
                  ))}
                </li>
              ))}
            </ul>
          )}
          {!grant.available && (
            <p className="text-muted-foreground">
              Scoped Runner access must be ready before you can save a CLI
              grant.
            </p>
          )}
        </div>
      )}
      {writeError && (
        <p role="alert" className="text-sm text-destructive">
          {writeError}
        </p>
      )}
      {pending && !busy && (
        <div className="space-y-2 rounded-lg border border-border p-3">
          <p className="text-sm text-muted-foreground">
            Check whether the Controller accepted the original{" "}
            {pending.body ? "grant update" : "revocation"}.
          </p>
          <Button
            type="button"
            size="sm"
            disabled={busy}
            onClick={() => void send()}
          >
            {busy ? "Checking…" : "Retry request"}
          </Button>
        </div>
      )}
      {mode && (
        <InlineEditorRegion editing>
          {stale && observation && (
            <div
              className="mb-3 flex flex-wrap items-center gap-2"
              role="status"
            >
              <p className="text-sm text-muted-foreground">
                The grant changed. Review the latest grant before continuing.
              </p>
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={busy || reading || Boolean(pending)}
                onClick={() => setRevision(observation.revision)}
              >
                Use current revision
              </Button>
            </div>
          )}
          {mode === "edit" ? (
            <CLIGrantFields
              choices={choices}
              initialScope={
                grant?.scope?.environment_id ??
                runner.environmentId ??
                undefined
              }
              initialActions={grant?.actions ?? []}
              disabled={
                busy ||
                reading ||
                stale ||
                Boolean(readError || pending) ||
                !grant?.available
              }
              locked={busy || Boolean(pending)}
              onCancel={() => toggle()}
              onSubmit={(body) => void send(body)}
            />
          ) : (
            <div className="space-y-3 rounded-lg border border-border bg-surface p-4">
              <p className="text-sm text-muted-foreground">
                Revocation blocks new CLI actions authorized by this grant.
                Tasks that were already accepted can continue.
              </p>
              <div className="flex flex-wrap justify-end gap-2">
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={busy || Boolean(pending)}
                  onClick={() => toggle()}
                >
                  Cancel
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant="destructive"
                  disabled={
                    busy ||
                    reading ||
                    stale ||
                    Boolean(readError || pending) ||
                    !grant?.actions.length
                  }
                  onClick={() => void send()}
                >
                  {busy ? "Revoking…" : "Revoke grant"}
                </Button>
              </div>
            </div>
          )}
        </InlineEditorRegion>
      )}
    </ResourcePanel>
  );
}
