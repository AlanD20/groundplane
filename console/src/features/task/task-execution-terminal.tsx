import { Disclosure } from "@/components/common/disclosure";
import { StatusBadge } from "@/components/common/status-badge";
import type { ActivityEntry, TaskStep } from "@/lib/types";
import { ChevronDown, Terminal } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { parseTaskEvent, type TaskEventResponse } from "./journal-model";
import { TaskExecutionSummary, taskDuration } from "./task-execution-summary";

type StreamState =
  "connecting" | "live" | "reconnecting" | "history" | "unavailable";

const terminalStatuses = ["completed", "failed", "timed_out", "aborted"];

export function TaskExecutionTerminal({ task }: { task: ActivityEntry }) {
  const [events, setEvents] = useState<TaskEventResponse[]>([]);
  const [streamState, setStreamState] = useState<StreamState>("connecting");
  const [eventError, setEventError] = useState<string>();
  const viewport = useRef<HTMLDivElement>(null);
  const following = useRef(true);
  const terminal = terminalStatuses.includes(task.status);
  const terminalRef = useRef(terminal);
  terminalRef.current = terminal;

  useEffect(() => {
    setEvents([]);
    setEventError(undefined);
    setStreamState(terminalRef.current ? "history" : "connecting");
    following.current = true;
    const source = new EventSource(
      `/api/v1/tasks/${encodeURIComponent(task.id)}/events`,
    );
    source.onopen = () => {
      setStreamState(terminalRef.current ? "history" : "live");
    };
    source.onmessage = (message) => {
      try {
        const event = parseTaskEvent(message.data);
        setEvents((current) =>
          current.some((item) => item.sequence === event.sequence)
            ? current
            : [...current, event]
                .sort((a, b) => a.sequence - b.sequence)
                .slice(-1000),
        );
      } catch {
        source.close();
        setStreamState("unavailable");
        setEventError("Unable to read a recorded execution event.");
      }
    };
    source.onerror = () => {
      if (terminalRef.current) {
        source.close();
        setStreamState("history");
      } else {
        setStreamState("reconnecting");
      }
    };
    return () => source.close();
  }, [task.id]);

  useEffect(() => {
    if (terminal)
      setStreamState((current) =>
        current === "unavailable" ? current : "history",
      );
  }, [terminal]);

  useEffect(() => {
    if (following.current && viewport.current)
      viewport.current.scrollTop = viewport.current.scrollHeight;
  }, [events]);

  const steps = task.steps ?? [];

  return (
    <section className="space-y-3" aria-label="Task execution">
      <TaskExecutionSummary task={task} events={events} />
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="space-y-1">
          <h3 className="flex items-center gap-2 text-sm font-medium">
            <Terminal className="size-4" aria-hidden />
            Execution
          </h3>
        </div>
        <StreamStatus
          state={streamState}
          taskStatus={task.status}
          terminal={terminal}
        />
      </div>

      <div className="overflow-hidden rounded-xl border border-slate-700 bg-slate-950 text-slate-200">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-800 px-4 py-2 text-xs text-slate-400">
          <span>Recorded execution</span>
          <span>
            {events.length} {events.length === 1 ? "transition" : "transitions"}
          </span>
        </div>
        <div
          ref={viewport}
          role="log"
          aria-live="polite"
          aria-relevant="additions"
          onScroll={() => {
            const element = viewport.current;
            if (element)
              following.current =
                element.scrollHeight -
                  element.scrollTop -
                  element.clientHeight <
                40;
          }}
          className="max-h-80 min-h-32 overflow-auto py-3 font-mono text-xs leading-6"
        >
          {!events.length && (
            <p className="px-4 text-slate-400">
              <time dateTime={task.updatedAt ?? task.ts}>
                {new Date(task.updatedAt ?? task.ts).toLocaleTimeString()}
              </time>{" "}
              · Task {task.status.replaceAll("_", " ")}
              {task.status === "pending"
                ? " · waiting for an executor"
                : " · no retained step transitions"}
            </p>
          )}
          {(events[0]?.sequence ?? 1) > 1 && (
            <p className="px-4 text-slate-400">
              Earlier transitions are no longer retained.
            </p>
          )}
          {events.map((event) => {
            const step = steps.find(
              (candidate) => candidate.label === event.step_id,
            );
            const name = step?.action || "Action not recorded";
            return (
              <div
                key={event.sequence}
                className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 px-4 py-1 even:bg-white/5 sm:grid-cols-[auto_5.5rem_minmax(0,1fr)]"
              >
                <time
                  className="shrink-0 text-slate-500"
                  dateTime={event.received_at}
                  title={new Date(event.received_at).toLocaleString()}
                >
                  {new Date(event.received_at).toLocaleTimeString()}
                </time>
                <span className={terminalStateTone(event.state)}>
                  {eventStateLabel(event.state)}
                </span>
                <span className="col-span-2 min-w-0 break-words [overflow-wrap:anywhere] sm:col-span-1">
                  {name}
                  {event.attempt > 1 ? ` | attempt ${event.attempt}` : ""}
                </span>
              </div>
            );
          })}
          {streamState === "reconnecting" && (
            <p role="status" className="text-amber-300">
              Reconnecting to execution updates…
            </p>
          )}
          {eventError && (
            <p role="alert" className="text-amber-300">
              {eventError}
            </p>
          )}
        </div>
      </div>

      <Disclosure
        summary={`Execution stages · ${steps.length}`}
        className="rounded-lg border border-border"
        summaryClassName="p-3 text-sm font-medium"
      >
        <div className="px-3 pb-3">
          {steps.length ? (
            <ol className="space-y-2">
              {steps.map((step, index) => (
                <li key={step.label}>
                  <TaskStepDetails
                    index={index}
                    step={step}
                    events={events.filter(
                      (event) => event.step_id === step.label,
                    )}
                  />
                </li>
              ))}
            </ol>
          ) : (
            <p className="rounded-lg border border-border bg-surface p-3 text-xs text-muted-foreground">
              No step plan was captured for this Task.
            </p>
          )}
        </div>
      </Disclosure>
    </section>
  );
}

function TaskStepDetails({
  index,
  step,
  events,
}: {
  index: number;
  step: TaskStep;
  events: TaskEventResponse[];
}) {
  const latest = events.at(-1);
  const state = latest?.state ?? stepState(step);
  const [expanded, setExpanded] = useState(false);
  const lastEventAt = latest?.received_at;
  const highestAttempt = events.reduce(
    (highest, event) => Math.max(highest, event.attempt),
    0,
  );
  const description =
    step.description ??
    "A description was not recorded when this Task was created.";
  const started = events
    .filter(
      (event) => event.attempt === latest?.attempt && event.state === "running",
    )
    .at(-1);
  const ended =
    latest && terminalStatuses.includes(latest.state) ? latest : undefined;
  const interval =
    started && ended
      ? Math.max(
          0,
          (Date.parse(ended.received_at) - Date.parse(started.received_at)) /
            1000,
        )
      : undefined;

  return (
    <Disclosure
      indicator={false}
      className="group overflow-hidden rounded-lg border border-border bg-card"
      open={expanded}
      onOpenChange={setExpanded}
      summaryClassName="flex cursor-pointer list-none items-start gap-3 px-3 py-3 focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring [&::-webkit-details-marker]:hidden"
      summary={
        <>
          <span className="flex size-6 shrink-0 items-center justify-center rounded-full border border-border bg-surface text-xs font-medium tabular-nums text-muted-foreground">
            {index + 1}
          </span>
          <span className="min-w-0 flex-1 space-y-1">
            <span className="block break-words text-sm font-medium [overflow-wrap:anywhere]">
              {step.action || "Action not recorded"}
            </span>
            <span className="block break-words text-xs text-muted-foreground [overflow-wrap:anywhere]">
              {step.target ??
                (latest
                  ? `Attempt ${latest.attempt}`
                  : "No execution event recorded")}
            </span>
          </span>
          <span className="flex shrink-0 items-center gap-2">
            <StatusBadge status={state} label={eventStateLabel(state)} />
            <ChevronDown
              className="size-3.5 text-muted-foreground transition-transform group-data-[open=true]:rotate-180"
              aria-hidden
            />
          </span>
        </>
      }
    >
      <div className="space-y-4 border-t border-border bg-surface/40 px-4 py-3">
        <p className="break-words text-xs text-muted-foreground [overflow-wrap:anywhere]">
          {description}
        </p>
        <dl className="grid grid-cols-1 gap-3 text-xs sm:grid-cols-2">
          <StepDatum label="Action" value={step.action || "Not recorded"} />
          <StepDatum label="Target" value={step.target || "Not recorded"} />
          <StepDatum
            label="Time limit"
            value={
              step.timeoutSeconds
                ? taskDuration(step.timeoutSeconds)
                : "Not recorded"
            }
          />
          <StepDatum
            label="Attempts"
            value={
              highestAttempt
                ? `Up to attempt ${highestAttempt}`
                : "Not recorded"
            }
          />
          <StepDatum
            label="Controller received latest event"
            value={
              lastEventAt
                ? new Date(lastEventAt).toLocaleString()
                : "Not recorded"
            }
          />
          {started && (
            <StepDatum
              label="Controller received start"
              value={new Date(started.received_at).toLocaleString()}
            />
          )}
          {interval !== undefined && (
            <StepDatum
              label="Recorded interval"
              value={taskDuration(interval)}
            />
          )}
          {step.detail && <StepDatum label="Script" value={step.detail} />}
        </dl>
        {events.length ? (
          <div className="space-y-2">
            <p className="text-xs font-medium">Retained transitions</p>
            <ol className="space-y-1 text-xs text-muted-foreground">
              {events.map((event) => (
                <li
                  key={event.sequence}
                  className="flex flex-wrap items-baseline gap-x-2 gap-y-1"
                >
                  <time dateTime={event.received_at}>
                    {new Date(event.received_at).toLocaleString()}
                  </time>
                  <span>· {eventStateLabel(event.state)}</span>
                  <span>· attempt {event.attempt}</span>
                </li>
              ))}
            </ol>
          </div>
        ) : (
          <p className="text-xs text-muted-foreground">
            No recorded events for this step.
          </p>
        )}
      </div>
    </Disclosure>
  );
}

function StepDatum({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="mt-1 break-words [overflow-wrap:anywhere]">{value}</dd>
    </div>
  );
}

function StreamStatus({
  state,
  taskStatus,
  terminal,
}: {
  state: StreamState;
  taskStatus: ActivityEntry["status"];
  terminal: boolean;
}) {
  if (state === "unavailable")
    return <StatusBadge status="unknown" label="Events unavailable" />;
  if (terminal)
    return <StatusBadge status="unknown" label="Recorded history" />;
  if (state === "reconnecting")
    return <StatusBadge status="degraded" label="Reconnecting" />;
  if (taskStatus === "pending")
    return <StatusBadge status="pending" label="Waiting for executor" />;
  if (state === "live")
    return <StatusBadge status="running" label="Live updates" />;
  return <StatusBadge status="pending" label="Connecting" />;
}

const failureStates = ["failed", "timed_out", "aborted"];

function stepState(step: TaskStep) {
  return step.state === "done" ? "completed" : step.state;
}

function eventStateLabel(state: string) {
  if (state === "completed" || state === "done") return "Completed";
  if (state === "timed_out") return "Timed out";
  if (state === "aborted") return "Aborted";
  if (state === "pending") return "Waiting";
  if (state === "running") return "Running";
  if (state === "failed") return "Failed";
  return "Unknown";
}

function terminalStateTone(state: TaskEventResponse["state"]) {
  if (state === "completed") return "text-emerald-400";
  if (failureStates.includes(state)) return "text-rose-400";
  if (state === "running") return "text-violet-300";
  return "text-amber-300";
}
