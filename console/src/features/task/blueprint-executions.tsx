import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Select } from "@/components/ui/select";
import type { ActivityEntry } from "@/lib/types";
import { TaskStepDetails } from "./task-execution-terminal";
import { taskStepFromAPI } from "./journal-model";
import { TaskExecutionSummary } from "./task-execution-summary";

export function BlueprintExecutions({
  task,
  limit,
  previous,
  loading,
  onLimit,
  onPrevious,
  onNext,
}: {
  task: ActivityEntry;
  limit: number;
  previous: boolean;
  loading: boolean;
  onLimit: (limit: number) => void;
  onPrevious: () => void;
  onNext: (cursor: string) => void;
}) {
  const page = task.executions;
  return (
    <section className="space-y-3" aria-label="Blueprint resource changes">
      <TaskExecutionSummary task={task} events={[]} />
      <div className="flex items-center justify-between gap-3">
        <h3 className="text-sm font-medium">Resource changes</h3>
        <Select
          aria-label="Resource changes per page"
          disabled={loading}
          className="h-8 w-20"
          value={String(limit)}
          options={[5, 10, 25, 50].map((value) => ({
            value: String(value),
            label: String(value),
          }))}
          onValueChange={(value) => onLimit(Number(value))}
        />
      </div>
      {!page?.items?.length && (
        <p className="text-xs text-muted-foreground">
          No retained resource execution records on this page.
        </p>
      )}
      {page?.items?.map((unit) => (
        <Disclosure
          key={unit.id}
          className="rounded-lg border border-border"
          summaryClassName="p-3 text-sm"
          summary={
            <>
              <span className="min-w-0 flex-1 break-words">
                {unit.type.replaceAll("_", " ")} ·{" "}
                {unit.target_name || unit.target}
              </span>
              <StatusBadge status={unit.status} />
            </>
          }
        >
          <div className="space-y-3 px-3 pb-3">
            <p className="text-xs text-muted-foreground">
              {unit.started_at
                ? `Started ${new Date(unit.started_at).toLocaleString()}`
                : "Not started"}
              {unit.finished_at
                ? ` · Finished ${new Date(unit.finished_at).toLocaleString()}`
                : ""}
            </p>
            {unit.failure && (
              <p role="alert" className="text-xs text-destructive">
                {unit.failure.summary}
              </p>
            )}
            {unit.recovery && (
              <p className="text-xs text-muted-foreground">
                {unit.recovery.summary}
              </p>
            )}
            {unit.reconciliation_required && (
              <p className="text-xs text-destructive">
                Restoration or cleanup is not yet proven.
              </p>
            )}
            <ol className="space-y-2">
              {unit.steps?.map((step, index) => (
                <li key={step.name}>
                  <TaskStepDetails
                    index={index}
                    events={[]}
                    step={taskStepFromAPI(step)}
                    terminal={[
                      "completed",
                      "failed",
                      "timed_out",
                      "aborted",
                    ].includes(unit.status)}
                  />
                </li>
              ))}
            </ol>
          </div>
        </Disclosure>
      ))}
      <div className="flex justify-end gap-2">
        <Button
          variant="outline"
          size="sm"
          disabled={loading || !previous}
          onClick={onPrevious}
        >
          Previous
        </Button>
        <Button
          variant="outline"
          size="sm"
          disabled={loading || !page?.next_cursor}
          onClick={() => page?.next_cursor && onNext(page.next_cursor)}
        >
          Next
        </Button>
      </div>
    </section>
  );
}
