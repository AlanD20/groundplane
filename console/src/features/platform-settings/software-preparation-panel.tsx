import { formatTimestamp } from "@/lib/format-timestamp";
import { FormActions } from "@/components/common/form-actions";
import { editorFooterClassName } from "@/components/common/workspace-section";
import { useEffect, useState } from "react";
import { ResourcePanel } from "@/components/common/resource-panel";
import { ResourceTable } from "@/components/common/resource-table";
import { TaskLink } from "@/components/common/task-link";
import { SoftwareIdentity } from "@/features/image-delivery/software-identity";
import {
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { controllerRequest } from "@/lib/controller-json-request";
import type { SoftwareReleaseCatalog, SoftwareRequest } from "./software-api";
import { useSoftwarePreparations } from "./use-software-preparations";
import { TaskRunAfterSelector } from "@/features/task/task-run-after-selector";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";

export function SoftwarePreparationPanel({ active }: { active: boolean }) {
  const work = useSoftwarePreparations(active);
  const [selection, setSelection] =
    useState<SoftwareRequest["selection"]>("both");
  const [source, setSource] =
    useState<SoftwareRequest["source_kind"]>("source_ref");
  const [ref, setRef] = useState("main");
  const [releases, setReleases] = useState<SoftwareReleaseCatalog["items"]>([]);
  const [releaseError, setReleaseError] = useState<string | null>(null);
  const [loadingReleases, setLoadingReleases] = useState(true);
  const [catalogAttempt, setCatalogAttempt] = useState(0);
  const [review, setReview] = useState<string | null>(null);
  const [after, setAfter] = useState<string[]>([]);
  const [afterLoading, setAfterLoading] = useState(false);
  const table = useTableView(
    work.items,
    {
      ref: (item) => item.ref,
      created: (item) => item.created_at,
      selection: (item) => item.selection,
      phase: (item) => item.phase,
    },
    "created",
    "desc",
  );
  useEffect(() => {
    if (!active || source !== "release") return;
    const abort = new AbortController();
    setLoadingReleases(true);
    setReleaseError(null);
    void controllerRequest<SoftwareReleaseCatalog>(
      `/software/releases?selection=${selection}`,
      200,
      { signal: abort.signal },
    )
      .then((catalog) => {
        if (!abort.signal.aborted) setReleases(catalog.items ?? []);
      })
      .catch((cause) => {
        if (!abort.signal.aborted)
          setReleaseError(
            cause instanceof Error ? cause.message : "Unable to list releases",
          );
      })
      .finally(() => {
        if (!abort.signal.aborted) setLoadingReleases(false);
      });
    return () => abort.abort();
  }, [active, source, selection, catalogAttempt]);
  const busy = work.publishing || work.pending !== null;
  const manualRelease =
    source === "release" &&
    !loadingReleases &&
    (releaseError !== null || releases.length === 0);
  const tagPrefix =
    selection === "all" || selection === "both" ? "v" : `${selection}/v`;
  const target = ref.trim();
  const canPrepare =
    !busy &&
    !afterLoading &&
    (source === "source_ref"
      ? Boolean(target)
      : manualRelease
        ? target.startsWith(tagPrefix) &&
          /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$/.test(
            target.slice(tagPrefix.length),
          )
        : !loadingReleases &&
          releases.some((release) => release.ref === target));
  return (
    <div className="flex flex-col gap-5">
      <ResourcePanel title="Prepare software">
        <form
          className="flex flex-col gap-4"
          onSubmit={(event) => {
            event.preventDefault();
            if (canPrepare)
              void work.prepare({
                selection,
                source_kind: source,
                ref: target,
                after,
              });
          }}
        >
          <div className="grid gap-4 md:grid-cols-3">
            <div className="space-y-2">
              <Label htmlFor="software-components">Components</Label>
              <Select
                id="software-components"
                value={selection}
                disabled={busy}
                options={[
                  {
                    value: "all",
                    label: "Controller, Agent, and Console",
                  },
                  { value: "both", label: "Controller and Agent" },
                  { value: "controller", label: "Controller only" },
                  { value: "agent", label: "Agent only" },
                  { value: "console", label: "Console only" },
                ]}
                onValueChange={(value) => {
                  const next = value as SoftwareRequest["selection"];
                  setSelection(next);
                  setLoadingReleases(true);
                  if (next === "console") {
                    setSource("release");
                    setRef("");
                  } else if (source === "release") setRef("");
                }}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="software-source">Source</Label>
              <Select
                id="software-source"
                value={source}
                disabled={busy}
                options={
                  selection === "console"
                    ? [{ value: "release", label: "Published release" }]
                    : [
                        {
                          value: "source_ref",
                          label: "Build branch, tag or commit",
                        },
                        { value: "release", label: "Published release" },
                      ]
                }
                onValueChange={(value) => {
                  setSource(value as SoftwareRequest["source_kind"]);
                  setRef(value === "source_ref" ? "main" : "");
                  if (value === "release") setLoadingReleases(true);
                }}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="software-ref">
                {source === "source_ref"
                  ? "Git ref"
                  : manualRelease
                    ? "Release tag"
                    : "Release"}
              </Label>
              {source === "source_ref" ? (
                <Input
                  id="software-ref"
                  value={ref}
                  disabled={busy}
                  onChange={(event) => setRef(event.target.value)}
                  required
                />
              ) : manualRelease ? (
                <Input
                  id="software-ref"
                  value={ref}
                  placeholder={`${tagPrefix}0.0.1`}
                  disabled={busy}
                  onChange={(event) => setRef(event.target.value)}
                  required
                />
              ) : (
                <Select
                  id="software-ref"
                  value={ref}
                  disabled={busy || loadingReleases}
                  searchable
                  placeholder={
                    loadingReleases ? "Loading releases…" : "Choose a release"
                  }
                  options={(releases ?? []).map((release) => ({
                    value: release.ref,
                    label: release.name || release.ref,
                  }))}
                  onValueChange={setRef}
                />
              )}
            </div>
          </div>
          {selection === "console" && (
            <p className="text-xs text-muted-foreground">
              Console-only updates use verified published component releases.
            </p>
          )}
          {selection === "all" && (
            <p className="text-xs text-muted-foreground">
              All stages Controller, Agent, and Console from one source commit.
              The Console and Controller must target the same API.
            </p>
          )}
          {releaseError && (
            <div className="flex flex-wrap items-center gap-2">
              <p
                role="alert"
                title={releaseError}
                className="text-sm text-muted-foreground"
              >
                The release list is unavailable. Enter a published tag to
                continue.
              </p>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={busy}
                onClick={() => {
                  setLoadingReleases(true);
                  setCatalogAttempt((attempt) => attempt + 1);
                }}
              >
                Retry list
              </Button>
            </div>
          )}
          <fieldset disabled={busy}>
            <TaskRunAfterSelector
              value={after}
              onChange={setAfter}
              onBusyChange={setAfterLoading}
            />
          </fieldset>
          <FormActions
            onDiscard={() => setRef("")}
            pinned={!!ref.trim()}
            status="Prepare version"
            className={editorFooterClassName}
          >
            <Button type="submit" disabled={!canPrepare}>
              {work.publishing
                ? "Submitting…"
                : source === "source_ref"
                  ? "Build version"
                  : "Download release"}
            </Button>
          </FormActions>
        </form>
        {work.pending && (
          <div className="mt-4 flex flex-wrap items-center gap-3 rounded-lg border p-3">
            <p className="text-sm">Request status unknown.</p>
            <Button
              variant="outline"
              disabled={work.publishing}
              onClick={() => void work.resolve()}
            >
              Resolve request
            </Button>
          </div>
        )}
        {work.error && (
          <p role="alert" className="mt-3 text-sm text-destructive">
            {work.error}
          </p>
        )}
      </ResourcePanel>
      {work.acceptedTaskId && (
        <ResourcePanel
          title="Accepted software Task"
          actions={<TaskLink taskId={work.acceptedTaskId} />}
        >
          <p className="text-sm">
            {work.acceptedStatus
              ? work.acceptedStatus.replaceAll("_", " ")
              : "Accepted"}
          </p>
          {work.trackingError && (
            <p role="status" className="text-sm text-muted-foreground">
              Could not refresh this Task. Checking again… {work.trackingError}
            </p>
          )}
          {work.acceptedKind === "prepare" &&
            !["failed", "cancelled", "superseded"].includes(
              work.acceptedStatus ?? "",
            ) && (
              <Button
                variant="outline"
                size="sm"
                disabled={busy}
                onClick={() => setReview(work.acceptedTaskId)}
              >
                Review & apply
              </Button>
            )}
        </ResourcePanel>
      )}
      {work.activation && (
        <ResourcePanel
          title="Latest activation"
          actions={<TaskLink taskId={work.activation.task_id} />}
        >
          <p className="text-sm">
            {work.activation.phase.replaceAll("_", " ")}
          </p>
          <div className="mt-3 flex flex-wrap gap-4 text-sm">
            {work.activation.controller_work_item_id && (
              <TaskLink taskId={work.activation.task_id}>
                Controller{" "}
                {work.activation.controller_applied ? "applied" : "update"}
              </TaskLink>
            )}
            {work.activation.agent_work_item_id && (
              <TaskLink taskId={work.activation.task_id}>
                Agent {work.activation.agent_applied ? "applied" : "update"}
              </TaskLink>
            )}
            {work.activation.console_work_item_id && (
              <TaskLink taskId={work.activation.task_id}>
                Console {work.activation.console_applied ? "applied" : "update"}
              </TaskLink>
            )}
          </div>
          {work.activation.error_detail && (
            <p role="alert" className="mt-3 text-sm text-destructive">
              {work.activation.error_detail}
            </p>
          )}
        </ResourcePanel>
      )}
      <TaskRunnerDialog
        open={review !== null}
        onOpenChange={(open) => {
          if (!open) setReview(null);
        }}
        title="Apply prepared software"
        description="Controller updates may briefly disconnect the Console. A Console-only update switches verified assets without restarting the Controller or Agent."
        type="update"
        target={review ?? ""}
        workspace="Platform"
        runAfter
        startLabel="Apply"
        startDisabled={(after) =>
          busy ||
          (!work.items.some(
            (item) => item.task_id === review && item.phase === "verified",
          ) &&
            !after.includes(review ?? ""))
        }
        review={
          review ? (
            <div className="space-y-2">
              <TaskLink taskId={review}>Selected preparation</TaskLink>
              {!work.items.some(
                (item) => item.task_id === review && item.phase === "verified",
              ) && (
                <p className="text-xs text-muted-foreground">
                  Select this preparation Task below so activation waits for its
                  verified result.
                </p>
              )}
            </div>
          ) : undefined
        }
        onDispatch={(after) =>
          review
            ? work.apply(review, after)
            : Promise.reject(new Error("No preparation selected"))
        }
      />
      <ResourcePanel
        title="Preparations"
        actions={
          <Button
            size="sm"
            variant="outline"
            disabled={work.loading}
            onClick={() => void work.refresh()}
          >
            Refresh
          </Button>
        }
      >
        {work.historyError && (
          <p role="alert" className="text-sm text-destructive">
            Could not refresh preparations. {work.historyError}
          </p>
        )}
        {!work.loaded && work.loading ? (
          <p className="text-sm text-muted-foreground">Loading preparations…</p>
        ) : (
          <>
            <ResourceTable>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableSortHead sort={table} field="ref">
                      Prepared candidates
                    </TableSortHead>
                    <TableSortHead sort={table} field="selection">
                      Components
                    </TableSortHead>
                    <TableSortHead sort={table} field="created">
                      Created
                    </TableSortHead>
                    <TableSortHead sort={table} field="phase">
                      Status
                    </TableSortHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {table.rows.map((item) => (
                    <TableRow key={item.task_id}>
                      <TableCell>
                        <div className="space-y-3">
                          <p className="text-xs text-muted-foreground">
                            {item.source_kind === "source_ref"
                              ? `Requested source ${item.ref}`
                              : `Published release ${item.ref}`}
                          </p>
                          {preparedComponents(item.selection).map(
                            (component) => {
                              const provenance = item.provenance.find(
                                (source) => source.component === component,
                              );
                              const artifact = item.artifacts.find(
                                (candidate) =>
                                  candidate.component === component,
                              );
                              return (
                                <div key={component} className="space-y-1">
                                  <p className="text-xs font-semibold capitalize text-muted-foreground">
                                    {component}
                                  </p>
                                  <SoftwareIdentity
                                    version={
                                      item.source_kind === "release"
                                        ? item.ref
                                        : undefined
                                    }
                                    sourceBuild={
                                      item.source_kind === "source_ref"
                                    }
                                    sourceCommit={provenance?.commit}
                                    artifactReference={artifact?.reference}
                                    digest={artifact?.manifest_digest}
                                    unavailable="Identity pending verification"
                                  />
                                </div>
                              );
                            },
                          )}
                        </div>
                      </TableCell>
                      <TableCell>{selectionLabel(item.selection)}</TableCell>
                      <TableCell>
                        {formatTimestamp(item.created_at, "Not recorded", {
                          seconds: true,
                        })}
                      </TableCell>
                      <TableCell>
                        <p>{item.phase.replaceAll("_", " ")}</p>
                        {item.error_detail && (
                          <p className="mt-1 max-w-sm text-xs text-destructive">
                            {item.error_detail}
                          </p>
                        )}
                      </TableCell>
                      <TableCell>
                        <div className="flex flex-col items-start gap-2">
                          <TaskLink taskId={item.task_id} />
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={busy || item.phase === "failed"}
                            onClick={() => setReview(item.task_id)}
                          >
                            Review & apply
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                  {table.total === 0 && (
                    <TableRow>
                      <TableCell colSpan={5} className="text-muted-foreground">
                        No software prepared yet.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </ResourceTable>
            <TablePagination table={table} label="preparations" />
          </>
        )}
        {work.nextCursor && (
          <Button
            className="mt-3"
            variant="outline"
            disabled={work.loading}
            onClick={() => void work.loadOlder()}
          >
            Load older preparations
          </Button>
        )}
      </ResourcePanel>
    </div>
  );
}

function preparedComponents(selection: SoftwareRequest["selection"]) {
  if (selection === "both") return ["controller", "agent"] as const;
  if (selection === "all") return ["controller", "agent", "console"] as const;
  return [selection] as const;
}

function selectionLabel(selection: SoftwareRequest["selection"]) {
  if (selection === "both") return "Controller + Agent";
  if (selection === "all") return "Controller + Agent + Console";
  return `${selection[0].toUpperCase()}${selection.slice(1)} only`;
}
