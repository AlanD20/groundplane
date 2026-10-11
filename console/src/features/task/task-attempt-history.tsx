import { Disclosure } from "@/components/common/disclosure";
import { ResourceTable } from "@/components/common/resource-table";
import { StatusBadge } from "@/components/common/status-badge";
import {
  TablePagination,
  TableSortHead,
  useTableView,
} from "@/components/common/table-controls";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { ActivityEntry } from "@/lib/types";
import { TaskFailureDetails } from "./task-failure-details";
import { taskDuration } from "./task-duration";

type History = NonNullable<ActivityEntry["attemptHistory"]>;
type Summary = NonNullable<History["summaries"]>[number];

export function TaskAttemptHistory({
  task,
  history,
}: {
  task: ActivityEntry;
  history: History;
}) {
  const summaries = history.summaries ?? [];
  const table = useTableView(
    summaries,
    {
      action: (value) => value.action,
      state: (value) => value.state,
      received: (value) => Date.parse(value.last_received_at),
    },
    "received",
    "desc",
    task.id,
  );

  return (
    <Disclosure
      summary={`Earlier attempt summaries · ${summaries.length}`}
      className="rounded-lg border border-border"
      summaryClassName="p-3 text-sm font-medium"
    >
      <div className="space-y-3 px-3 pb-3">
        <p className="text-xs text-muted-foreground">
          Recorded observations retained after detailed events were trimmed.
          Times are when the Controller received them; missing outcomes are not
          proof of success or recovery.
        </p>
        {history.dropped_summaries > 0 && (
          <p className="text-xs text-muted-foreground">
            {history.dropped_summaries} older summaries exceeded the history
            limit and are no longer retained.
          </p>
        )}
        <ResourceTable>
          <Table>
            <TableHeader>
              <TableRow>
                <TableSortHead field="action" sort={table}>
                  Action
                </TableSortHead>
                <TableSortHead field="state" sort={table}>
                  Last recorded outcome
                </TableSortHead>
                <TableSortHead field="received" sort={table}>
                  Last received
                </TableSortHead>
                <TableHead>Details</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {table.rows.map((summary) => (
                <TableRow key={summary.first_sequence}>
                  <TableCell className="min-w-40">
                    <span className="block break-words font-medium [overflow-wrap:anywhere]">
                      {summary.action}
                    </span>
                    <span className="block text-xs text-muted-foreground">
                      Attempt {summary.attempt}
                    </span>
                    {summary.target && (
                      <span className="block break-all text-xs text-muted-foreground">
                        {summary.target}
                      </span>
                    )}
                  </TableCell>
                  <TableCell>
                    <StatusBadge
                      status={
                        summary.state === "running" ||
                        summary.state === "pending"
                          ? "unknown"
                          : summary.state
                      }
                      label={
                        summary.state === "running" ||
                        summary.state === "pending"
                          ? "No outcome retained"
                          : summary.state.replaceAll("_", " ")
                      }
                    />
                  </TableCell>
                  <TableCell className="whitespace-nowrap text-xs text-muted-foreground">
                    <time dateTime={summary.last_received_at}>
                      {new Date(summary.last_received_at).toLocaleString()}
                    </time>
                  </TableCell>
                  <TableCell className="min-w-40">
                    <AttemptDetails summary={summary} task={task} />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </ResourceTable>
        <TablePagination table={table} label="Attempt summaries" />
      </div>
    </Disclosure>
  );
}

function AttemptDetails({
  summary,
  task,
}: {
  summary: Summary;
  task: ActivityEntry;
}) {
  const started = summary.started_received_at
    ? Date.parse(summary.started_received_at)
    : undefined;
  const finished = summary.finished_received_at
    ? Date.parse(summary.finished_received_at)
    : undefined;
  const duration =
    started !== undefined && finished !== undefined && finished >= started
      ? (finished - started) / 1000
      : undefined;
  return (
    <Disclosure
      summary="Recorded details"
      summaryClassName="text-xs text-muted-foreground"
    >
      <dl className="mt-2 space-y-1 text-xs text-muted-foreground">
        <div>
          <dt className="inline">Start received: </dt>
          <dd className="inline">
            {summary.started_received_at
              ? new Date(summary.started_received_at).toLocaleString()
              : "Not retained"}
          </dd>
        </div>
        <div>
          <dt className="inline">Outcome received: </dt>
          <dd className="inline">
            {summary.finished_received_at
              ? new Date(summary.finished_received_at).toLocaleString()
              : "Not retained"}
          </dd>
        </div>
        {duration !== undefined && (
          <div>
            <dt className="inline">Receipt interval: </dt>
            <dd className="inline">{taskDuration(duration)}</dd>
          </div>
        )}
        <div>
          <dt className="inline">Observed events: </dt>
          <dd className="inline">
            {summary.first_sequence}–{summary.last_sequence}
          </dd>
        </div>
        <div>
          <dt className="inline">Last ordinal: </dt>
          <dd className="inline">{summary.ordinal}</dd>
        </div>
        {summary.step_id && (
          <div>
            <dt>Step</dt>
            <dd className="break-all font-mono">{summary.step_id}</dd>
          </div>
        )}
      </dl>
      {summary.failure && (
        <div className="mt-2">
          <TaskFailureDetails
            failure={summary.failure}
            steps={task.steps ?? []}
            label="Recorded failure"
          />
        </div>
      )}
    </Disclosure>
  );
}
