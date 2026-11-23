import { ArrowLeft, ChevronRight, History } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ContextLink } from "@/components/common/context-link";
import { EmptyState } from "@/components/common/empty-state";
import { PageHeader } from "@/components/common/page-header";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { requestTask, type TaskResponse } from "@/features/task/api";
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

type WorkPage = { after?: string; revision?: number };

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
  const [pages, setPages] = useState<WorkPage[]>([{}]);
  const [loading, setLoading] = useState(true);
  const [shownPage, setShownPage] = useState(1);
  const [reload, setReload] = useState(0);
  const [selectedId, setSelectedId] = useState<string>();
  const backLink = useRef<HTMLAnchorElement>(null);
  const page = pages[pages.length - 1];
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
    const refresh = createTaskDetailRefresh(
      async () => {
        try {
          const next = await requestTask(taskId, controller.signal, {
            limit: 25,
            ...page,
          });
          if (controller.signal.aborted) return;
          setDetail(next);
          setShownPage(pages.length);
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
      },
      page.revision ? undefined : 2000,
    );
    if (!page.revision) {
      stopEvents = watchTaskEvents(taskId, refresh.invalidate, () => {
        // Keep the last snapshot visible while fallback reads reconnect.
      });
    }
    void refresh.start();
    return () => {
      controller.abort();
      refresh.stop();
      stopEvents();
    };
  }, [linkedRun, page, pages.length, reload, taskId, watchTaskEvents]);

  function changePage(next: WorkPage[]) {
    setLoading(true);
    setError(undefined);
    setSelectedId(undefined);
    setPages(next);
  }

  const task = detail?.task;
  const work = detail?.work.items ?? [];
  const selected =
    work.find((item) => item.id === selectedId) ??
    work.find(
      (item) => item.outcome === "failed" || item.phase === "attention",
    ) ??
    work.find((item) => serviceId && item.target_id === serviceId) ??
    work.find(
      (item) => item.phase === "running" || item.phase === "assigned",
    ) ??
    work[0];
  function loadLatest() {
    changePage([{}]);
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
            ) : selected && !detail.preparation ? (
              <>
                <TaskDependencyStatus task={task} />
                <div className="grid min-w-0 items-start gap-4 lg:grid-cols-[minmax(15rem,19rem)_minmax(0,1fr)]">
                  <Card className="min-w-0 gap-0">
                    <CardHeader>
                      <CardTitle>Operations</CardTitle>
                    </CardHeader>
                    <CardContent className="px-2 py-2">
                      <Select
                        className="lg:hidden"
                        aria-label="Select operation"
                        value={selected.id}
                        onValueChange={setSelectedId}
                        options={work.map((item) => ({
                          value: item.id,
                          label: [
                            item.target_name,
                            operationLabel(item.operation),
                          ]
                            .filter(Boolean)
                            .join(" · "),
                        }))}
                      />
                      <ul
                        aria-label="Deployment operations"
                        className="hidden space-y-1 lg:block"
                      >
                        {work.map((item) => (
                          <li key={item.id}>
                            <button
                              type="button"
                              aria-pressed={item.id === selected.id}
                              onClick={() => setSelectedId(item.id)}
                              className="flex w-full min-w-0 items-center justify-between gap-2 rounded-lg border border-transparent px-3 py-2.5 text-left text-sm hover:bg-muted/60 focus-visible:outline-2 focus-visible:outline-ring aria-pressed:border-border aria-pressed:bg-accent"
                            >
                              <span className="min-w-0 space-y-1 [overflow-wrap:anywhere]">
                                <span className="block font-medium">
                                  {item.target_name ||
                                    operationLabel(item.operation)}
                                </span>
                                {item.target_name && (
                                  <span className="block text-xs text-muted-foreground">
                                    {operationLabel(item.operation)}
                                  </span>
                                )}
                              </span>
                              <StatusBadge
                                status={item.outcome || item.phase}
                              />
                            </button>
                          </li>
                        ))}
                      </ul>
                      {(page.revision || detail.work.more) && (
                        <div className="mt-2 flex flex-wrap items-center justify-between gap-2 border-t border-border px-2 pt-2">
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={loading || pages.length === 1}
                            onClick={() => changePage(pages.slice(0, -1))}
                          >
                            Previous
                          </Button>
                          <span className="text-xs text-muted-foreground">
                            Page {shownPage}
                          </span>
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={loading || !detail.work.more}
                            onClick={() =>
                              changePage([
                                ...pages.map((entry) => ({
                                  ...entry,
                                  revision: detail.work.revision,
                                })),
                                {
                                  after: detail.work.after,
                                  revision: detail.work.revision,
                                },
                              ])
                            }
                          >
                            Next
                          </Button>
                        </div>
                      )}
                      {page.revision && (
                        <div className="flex flex-wrap items-center justify-between gap-2 px-2 pt-2 text-xs text-muted-foreground">
                          <span>Fixed snapshot</span>
                          <Button size="sm" variant="link" onClick={loadLatest}>
                            Show latest
                          </Button>
                        </div>
                      )}
                    </CardContent>
                  </Card>
                  <section
                    aria-label="Selected operation"
                    className="min-w-0 space-y-3 rounded-xl bg-card"
                  >
                    <WorkInspection
                      key={selected.id}
                      taskId={taskId}
                      work={selected}
                      revision={detail.work.revision}
                      targetName={selected.target_name}
                      onChanged={loadLatest}
                    />
                    <WorkCheckEvidence task={task} work={selected} />
                  </section>
                </div>
              </>
            ) : (
              <TaskExecution detail={detail} onChanged={loadLatest} />
            ))}
          <Card>
            <CardHeader>
              <CardTitle>Service releases</CardTitle>
            </CardHeader>
            <CardContent className="space-y-1">
              {releaseList.map((release) => (
                <ContextLink
                  key={release.id}
                  to={`${basePath}/releases/${release.id}${serviceId ? `?service=${serviceId}` : ""}`}
                  returnLabel="deployment run"
                  className="flex min-w-0 items-center gap-3 rounded-md px-3 py-2 text-sm hover:bg-muted/60 focus-visible:outline-2 focus-visible:outline-ring aria-current:bg-accent"
                  aria-current={release.id === releaseId ? "true" : undefined}
                >
                  <span className="min-w-0 flex-1 break-words font-medium">
                    {release.service}
                  </span>
                  <span className="min-w-0 break-words text-xs text-muted-foreground">
                    {release.tag}
                  </span>
                  <StatusBadge
                    status={
                      release.status === "active" ? "succeeded" : release.state
                    }
                    label={
                      release.status === "active"
                        ? "Serving"
                        : release.state.replaceAll("_", " ")
                    }
                  />
                  <ChevronRight
                    aria-hidden
                    className="size-4 shrink-0 text-muted-foreground"
                  />
                </ContextLink>
              ))}
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
