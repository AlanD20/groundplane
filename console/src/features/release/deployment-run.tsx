import { ArrowLeft, ChevronLeft, ChevronRight, History } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { InspectionWorkspace } from "@/components/common/inspection-workspace";
import {
  CollectionToolbar,
  TablePagination,
  useTableView,
} from "@/components/common/table-controls";
import { ContextLink } from "@/components/common/context-link";
import { EmptyState } from "@/components/common/empty-state";
import { PageHeader } from "@/components/common/page-header";
import { StatusBadge, statusLabel } from "@/components/common/status-badge";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { TaskResponse } from "@/features/task/api";
import { requestDeploymentRun } from "./deployment-run-api";
import { taskFromAPI } from "@/features/task/journal-model";
import { operationLabel } from "@/features/task/operation-label";
import { TaskDependencyStatus } from "@/features/task/task-dependency-status";
import { createTaskDetailRefresh } from "@/features/task/task-detail-refresh";
import { TaskExecution } from "@/features/task/task-execution";
import { WorkCheckEvidence } from "@/features/task/work-check-evidence";
import { WorkInspection } from "@/features/task/work-inspection";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";
import {
  deploymentReleaseParam,
  deploymentRunLink,
} from "./deployment-run-navigation";

export function DeploymentRun({
  taskId,
  env,
  serviceId,
}: {
  taskId: string;
  env: Environment;
  serviceId?: string;
}) {
  const params = useRequiredParams("tenant", "project", "env");
  const [search] = useSearchParams();
  const { watchTaskEvents } = useStore();
  const [detail, setDetail] = useState<TaskResponse>();
  const [error, setError] = useState<string>();
  const [operationQuery, setOperationQuery] = useState("");
  const [loading, setLoading] = useState(true);
  const [reload, setReload] = useState(0);
  const [selectedId, setSelectedId] = useState<string>();
  const backLink = useRef<HTMLAnchorElement>(null);
  const releases = env.deploys.filter((release) => release.taskId === taskId);
  const releaseId = search.get(deploymentReleaseParam);
  const linkedRun = releases.some(
    (release) => !serviceId || release.serviceId === serviceId,
  );
  const basePath = `/t/${params.tenant}/${params.project}/${encodeURIComponent(params.env)}`;
  const releaseList = serviceId
    ? releases.filter((release) => release.serviceId === serviceId)
    : releases;

  useEffect(() => {
    backLink.current?.scrollIntoView({ block: "start" });
    backLink.current?.focus({ preventScroll: true });
  }, []);

  useEffect(() => {
    if (!linkedRun) return;
    const controller = new AbortController();
    let stopEvents = () => {};
    const refresh = createTaskDetailRefresh(async () => {
      try {
        const next = await requestDeploymentRun(taskId, controller.signal);
        if (controller.signal.aborted) return;
        setDetail(next);
        setError(undefined);
        if (next.task.finished_at) {
          refresh.stop();
          stopEvents();
        }
      } catch (cause) {
        if (!controller.signal.aborted) {
          setError(
            cause instanceof Error
              ? cause.message
              : "Unable to load deployment execution",
          );
        }
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    }, 2000);
    stopEvents = watchTaskEvents(taskId, refresh.invalidate, () => {
      // Keep the last snapshot visible while fallback reads reconnect.
    });
    void refresh.start();
    return () => {
      controller.abort();
      refresh.stop();
      stopEvents();
    };
  }, [linkedRun, reload, taskId, watchTaskEvents]);

  const task = detail?.task;
  const work = detail?.work.items ?? [];
  const searchTerms = operationQuery.trim().toLowerCase().split(/\s+/);
  const operations = useTableView(
    work.filter((item) => {
      const haystack = [
        item.target_name,
        item.target_id,
        operationLabel(item.operation),
        statusLabel(item.outcome || item.phase),
      ]
        .join(" ")
        .toLowerCase();
      return searchTerms.every((term) => haystack.includes(term));
    }),
    { created: (item) => `${Date.parse(item.created_at) || 0}/${item.id}` },
    "created",
    "asc",
    `${taskId}/${operationQuery}`,
  );
  const selected =
    operations.rows.find((item) => item.id === selectedId) ??
    operations.rows.find(
      (item) => item.outcome === "failed" || item.phase === "attention",
    ) ??
    operations.rows.find((item) => serviceId && item.target_id === serviceId) ??
    operations.rows.find(
      (item) => item.phase === "running" || item.phase === "assigned",
    ) ??
    operations.rows[0];
  const operationIndex = operations.sortedRows.findIndex(
    (item) => item.id === selected?.id,
  );
  function stepOperation(offset: number) {
    const index = operationIndex + offset;
    const item = operations.sortedRows[index];
    if (!item) return;
    operations.setPage(Math.floor(index / operations.pageSize));
    setSelectedId(item.id);
  }

  function loadLatest() {
    setError(undefined);
    setReload((value) => value + 1);
  }

  const counts = task?.counts;
  const summary =
    counts &&
    (
      [
        ["Succeeded", counts.succeeded],
        ["Running", counts.running],
        ["Queued", counts.ready + counts.assigned + counts.waiting],
        ["Failed", counts.failed],
        ["Needs attention", counts.attention],
        ["Cancelled", counts.cancelled],
        ["Superseded", counts.superseded],
      ] as const
    ).filter(([, count]) => count > 0);

  return (
    <div className="min-w-0 space-y-5">
      <Link
        ref={backLink}
        to={deploymentRunLink(search)}
        className="inline-flex scroll-mt-5 items-center gap-2 rounded-sm text-sm text-primary hover:underline focus-visible:outline-2 focus-visible:outline-ring"
      >
        <ArrowLeft aria-hidden className="size-4" /> Back to deployments
      </Link>
      <PageHeader
        eyebrow={`Deployment run · ${env.name}`}
        title={linkedRun && task ? taskFromAPI(task).title : "Deployment run"}
        icon={<History />}
        actions={
          linkedRun &&
          task && <TaskLink taskId={taskId}>Open full Task</TaskLink>
        }
        meta={
          linkedRun &&
          task && (
            <>
              <StatusBadge status={task.status} />
              <span>
                Requested {formatTimestamp(task.created_at, "Time unavailable")}
              </span>
              {task.finished_at && (
                <span>
                  Finished{" "}
                  {formatTimestamp(task.finished_at, "Time unavailable", {
                    seconds: true,
                  })}
                </span>
              )}
            </>
          )
        }
      />
      {!linkedRun ? (
        <EmptyState icon={<History />} title="Deployment run not found" />
      ) : (
        <>
          {summary && summary.length > 0 && (
            <ul
              aria-label="Operation results"
              className="flex flex-wrap gap-x-6 gap-y-2 rounded-xl border border-border bg-card px-4 py-3 text-sm"
            >
              {summary.map(([label, count]) => (
                <li key={label} className="text-muted-foreground">
                  <strong className="font-semibold tabular-nums text-foreground">
                    {count}
                  </strong>{" "}
                  {label}
                </li>
              ))}
            </ul>
          )}
          <Card>
            <CardHeader>
              <CardTitle id="deployment-service-releases">
                Service releases
              </CardTitle>
            </CardHeader>
            <Table
              aria-labelledby="deployment-service-releases"
              className="table-fixed"
            >
              <TableHeader>
                <TableRow>
                  <TableHead className="w-[30%] px-2 sm:px-5">
                    Service
                  </TableHead>
                  <TableHead className="w-[27%] px-2 sm:px-5">
                    Image tag
                  </TableHead>
                  <TableHead className="w-[22%] px-2 sm:px-5">
                    Strategy
                  </TableHead>
                  <TableHead className="px-2 sm:px-5">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {releaseList.map((release) => (
                  <TableRow
                    key={release.id}
                    className="even:bg-muted/35 even:hover:bg-muted/60"
                  >
                    <TableCell className="px-2 py-3 sm:px-5">
                      <ContextLink
                        to={`${basePath}/releases/${release.id}${serviceId ? `?service=${serviceId}` : ""}`}
                        returnLabel="deployment run"
                        className="inline-flex max-w-full items-center gap-1 rounded-sm font-medium text-primary hover:underline focus-visible:outline-2 focus-visible:outline-ring aria-current:bg-accent"
                        aria-current={
                          release.id === releaseId ? "true" : undefined
                        }
                      >
                        <span className="min-w-0 [overflow-wrap:anywhere]">
                          {release.service}
                        </span>
                        <ChevronRight
                          aria-hidden
                          className="size-3.5 shrink-0"
                        />
                      </ContextLink>
                    </TableCell>
                    <TableCell className="px-2 py-3 font-mono text-xs text-muted-foreground [overflow-wrap:anywhere] sm:px-5">
                      {release.tag || "—"}
                    </TableCell>
                    <TableCell className="px-2 py-3 text-xs capitalize [overflow-wrap:anywhere] sm:px-5">
                      {release.strategy}
                    </TableCell>
                    <TableCell className="px-2 py-3 sm:px-5">
                      <StatusBadge
                        status={
                          release.status === "active"
                            ? "succeeded"
                            : release.state
                        }
                        label={
                          release.status === "active"
                            ? "Serving"
                            : release.state.replaceAll("_", " ")
                        }
                        className="items-baseline whitespace-normal [overflow-wrap:anywhere] [&>span]:shrink-0"
                      />
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Card>
          {error && (
            <div
              role="alert"
              className="flex flex-wrap items-center gap-3 text-sm text-destructive"
            >
              <p>{error}</p>
              <Button size="sm" variant="outline" onClick={loadLatest}>
                Reload latest
              </Button>
            </div>
          )}
          {loading && !detail && (
            <EmptyState
              loading
              icon={<History />}
              title="Loading deployment run"
            />
          )}
          {detail &&
            task &&
            (task.history_expired ? (
              <p className="text-sm text-muted-foreground">
                Execution details are no longer retained.
              </p>
            ) : work.length > 0 && !detail.preparation ? (
              <>
                <TaskDependencyStatus task={task} />
                <InspectionWorkspace
                  title="Operations"
                  selectionLabel="Select operation"
                  detailsLabel="Selected operation"
                  emptyMessage="No operations match your search."
                  selectedId={selected?.id}
                  onSelect={setSelectedId}
                  items={operations.rows.map((item) => ({
                    id: item.id,
                    label: item.target_name || operationLabel(item.operation),
                    description: item.target_name
                      ? operationLabel(item.operation)
                      : undefined,
                    status: (
                      <StatusBadge
                        status={item.outcome || item.phase}
                        className="shrink-0"
                      />
                    ),
                  }))}
                  toolbar={
                    <CollectionToolbar
                      query={operationQuery}
                      onQueryChange={setOperationQuery}
                      label="operations"
                    >
                      <Select
                        aria-label="Operation order"
                        value={operations.direction}
                        options={[
                          { value: "asc", label: "Oldest first" },
                          { value: "desc", label: "Newest first" },
                        ]}
                        onValueChange={(direction) => {
                          if (direction !== operations.direction)
                            operations.sortBy("created");
                        }}
                      />
                    </CollectionToolbar>
                  }
                  footer={
                    operations.pages > 1 && (
                      <TablePagination label="Operations" table={operations} />
                    )
                  }
                >
                  {selected && (
                    <>
                      <nav
                        aria-label="Operation navigation"
                        className="flex flex-wrap items-center gap-2 border-b border-border pb-2"
                      >
                        <Button
                          variant="ghost"
                          size="icon-sm"
                          aria-label="Previous operation"
                          disabled={operationIndex === 0}
                          onClick={() => stepOperation(-1)}
                        >
                          <ChevronLeft aria-hidden />
                        </Button>
                        <span className="text-xs tabular-nums text-muted-foreground">
                          {operationIndex + 1} of {operations.total}
                        </span>
                        <Button
                          variant="ghost"
                          size="icon-sm"
                          aria-label="Next operation"
                          disabled={operationIndex + 1 >= operations.total}
                          onClick={() => stepOperation(1)}
                        >
                          <ChevronRight aria-hidden />
                        </Button>
                      </nav>
                      <div key={selected.id} className="space-y-3">
                        <WorkInspection
                          className="rounded-none border-0 p-0"
                          taskId={taskId}
                          work={selected}
                          revision={detail.work.revision}
                          targetName={selected.target_name}
                          onChanged={loadLatest}
                        />
                        <WorkCheckEvidence task={task} work={selected} />
                      </div>
                    </>
                  )}
                </InspectionWorkspace>
              </>
            ) : (
              <TaskExecution detail={detail} onChanged={loadLatest} />
            ))}
        </>
      )}
    </div>
  );
}
