import type { ActivityEntry } from "@/lib/types";
import { Terminal } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { parseTaskEvent, type TaskEventResponse } from "./journal-model";

export function TaskExecutionTerminal({ task }: { task: ActivityEntry }) {
  const [events, setEvents] = useState<TaskEventResponse[]>([]);
  const [error, setError] = useState<string>();
  const [connected, setConnected] = useState(false);
  const viewport = useRef<HTMLDivElement>(null);
  const following = useRef(true);
  const terminal = ["completed", "failed", "timed_out", "aborted"].includes(
    task.status,
  );

  useEffect(() => {
    setEvents([]);
    setError(undefined);
    following.current = true;
    const source = new EventSource(
      `/api/v1/tasks/${encodeURIComponent(task.id)}/events`,
    );
    source.onopen = () => {
      setConnected(true);
      setError(undefined);
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
        setConnected(false);
        setError("Unable to read the recorded execution event.");
      }
    };
    source.onerror = () => {
      setConnected(false);
      if (terminal) source.close();
      else setError("Connection interrupted. Reconnecting to recorded events…");
    };
    return () => {
      source.close();
      setConnected(false);
    };
  }, [task.id, terminal]);

  useEffect(() => {
    if (following.current && viewport.current)
      viewport.current.scrollTop = viewport.current.scrollHeight;
  }, [events]);

  return (
    <section className="space-y-2" aria-label="Task execution">
      <div className="flex items-center justify-between gap-3">
        <h3 className="flex items-center gap-2 text-sm font-medium">
          <Terminal className="size-4" />
          Execution
        </h3>
        <span className="text-xs text-muted-foreground">
          {terminal ? "Recorded history" : connected ? "Live" : "Connecting…"}
        </span>
      </div>
      <div className="overflow-hidden rounded-xl border border-slate-700 bg-slate-950 text-slate-200">
        <div className="flex items-center justify-between border-b border-slate-800 px-4 py-2 text-xs text-slate-400">
          <span>GP execution events</span>
          <span>{events.length} recorded</span>
        </div>
        <div
          ref={viewport}
          onScroll={() => {
            const element = viewport.current;
            if (element)
              following.current =
                element.scrollHeight -
                  element.scrollTop -
                  element.clientHeight <
                40;
          }}
          className="max-h-80 min-h-32 overflow-auto p-4 font-mono text-xs leading-6"
        >
          {!events.length && (
            <p className="text-slate-400">
              <time dateTime={task.updatedAt ?? task.ts}>
                {new Date(task.updatedAt ?? task.ts).toLocaleTimeString()}
              </time>{" "}
              · {task.status.replaceAll("_", " ")}
              {task.status === "pending"
                ? " · waiting for an executor"
                : " · no step transitions recorded"}
            </p>
          )}
          {events.map((event) => {
            const index =
              task.steps?.findIndex((step) => step.label === event.step_id) ??
              -1;
            const step = index >= 0 ? task.steps![index] : undefined;
            const name = step?.detail
              ? `Script ${step.detail}`
              : step && !/^[a-z]+_[A-Z0-9]{20,}$/.test(step.label)
                ? step.label.replaceAll("_", " ")
                : index >= 0
                  ? `Step ${index + 1}`
                  : "Execution step";
            const color =
              event.state === "completed"
                ? "text-emerald-400"
                : ["failed", "aborted", "timed_out"].includes(event.state)
                  ? "text-rose-400"
                  : "text-violet-300";
            return (
              <div key={event.sequence} className="flex gap-3">
                <time
                  className="shrink-0 text-slate-500"
                  dateTime={event.received_at}
                >
                  {new Date(event.received_at).toLocaleTimeString()}
                </time>
                <span className={`w-20 shrink-0 ${color}`}>{event.state}</span>
                <span className="min-w-0 break-words">
                  {name}
                  {event.attempt > 1 ? ` · attempt ${event.attempt}` : ""}
                </span>
              </div>
            );
          })}
          {error && (
            <p role="status" className="text-amber-300">
              {error}
            </p>
          )}
        </div>
      </div>
      <p className="text-xs text-muted-foreground">
        Recorded step transitions, not shell output.
      </p>
    </section>
  );
}
