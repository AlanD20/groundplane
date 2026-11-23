import { EmptyState } from "@/components/common/empty-state";
import { LogSource } from "./log-line";
import { LogRows } from "./log-rows";
("use client");

import { Button } from "@/components/ui/button";
import { CopyButton } from "@/components/common/copy-button";
import { Select } from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { useStore } from "@/lib/store";
import type { LogTarget, TransientLogEvent } from "@/lib/transient-logs";
import { ScrollText } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";

export function LogViewer({
  target,
  label = "Logs",
}: {
  target: LogTarget;
  label?: string;
}) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <Button variant="outline" onClick={() => setOpen(true)}>
        <ScrollText className="size-4" />
        {label}
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-5xl">
          <DialogHeader>
            <DialogTitle>{label}</DialogTitle>
            <DialogDescription>
              Reopen after deploying to select new containers.
            </DialogDescription>
          </DialogHeader>
          {open && <LogStream target={target} />}
        </DialogContent>
      </Dialog>
    </>
  );
}

export function LogStream({ target }: { target: LogTarget }) {
  const store = useStore();
  const [query, setQuery] = useState("");
  const [container, setContainer] = useState("all");
  const [stream, setStream] = useState("all");
  const [wrap, setWrap] = useState(true);
  const [atEnd, setAtEnd] = useState(true);
  const [tail, setTail] = useState(200);
  const [follow, setFollow] = useState(true);
  const [events, setEvents] = useState<TransientLogEvent[]>([]);
  const [received, setReceived] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [status, setStatus] = useState<
    | "Connecting"
    | "Streaming"
    | "Paused"
    | "Read complete"
    | "Stream closed"
    | "Disconnected"
  >("Connecting");
  const busy = status === "Connecting" || status === "Streaming";
  const outputRef = useRef<HTMLDivElement>(null);
  const pinnedToEnd = useRef(true);
  const abortRef = useRef<AbortController | null>(null);
  const pendingEvents = useRef<TransientLogEvent[]>([]);
  const flushTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const replaceLogSnapshot = useRef(false);

  const flushEvents = () => {
    if (flushTimer.current !== null) {
      clearTimeout(flushTimer.current);
      flushTimer.current = null;
    }
    const batch = pendingEvents.current;
    pendingEvents.current = [];
    if (batch.length > 0) {
      const replace = replaceLogSnapshot.current;
      replaceLogSnapshot.current = false;
      setEvents((current) =>
        (replace ? batch : [...current, ...batch]).slice(-1000),
      );
      setReceived((current) => (replace ? 0 : current) + batch.length);
    }
  };

  const stop = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    flushEvents();
  };

  const start = (newTarget = false) => {
    stop();
    const controller = new AbortController();
    abortRef.current = controller;
    replaceLogSnapshot.current = true;
    setStatus("Connecting");
    setError(null);
    if (newTarget) {
      setEvents([]);
      setReceived(0);
      pinnedToEnd.current = true;
      setAtEnd(true);
    }
    void store
      .watchLogs(
        target,
        {
          tail,
          follow,
          signal: controller.signal,
          onOpen: () => {
            if (!controller.signal.aborted) setStatus("Streaming");
          },
        },
        (event) => {
          if (controller.signal.aborted) return;
          pendingEvents.current.push(event);
          if (flushTimer.current === null) {
            flushTimer.current = setTimeout(flushEvents, 50);
          }
        },
      )
      .then(() => {
        if (controller.signal.aborted) return;
        flushEvents();
        if (replaceLogSnapshot.current) {
          replaceLogSnapshot.current = false;
          setEvents([]);
          setReceived(0);
        }
        setStatus(follow ? "Stream closed" : "Read complete");
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          setStatus("Disconnected");
          setError(
            reason instanceof Error ? reason.message : "Log stream failed",
          );
        }
      })
      .finally(() => {
        if (abortRef.current === controller) {
          flushEvents();
          abortRef.current = null;
        }
      });
  };

  useEffect(() => {
    setContainer("all");
    start(true);
    return stop;
    // A new target owns a new stream; filters never restart it.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target.kind, target.id]);
  const visible = useMemo(
    () =>
      events.filter(
        (event) =>
          (container === "all" || event.container_id === container) &&
          (stream === "all" || event.stream === stream) &&
          event.line.toLowerCase().includes(query.toLowerCase()),
      ),
    [events, container, stream, query],
  );
  const containers = useMemo(
    () => [
      ...new Map(
        events.map((event) => [event.container_id, event.container_name]),
      ).entries(),
    ],
    [events],
  );
  return (
    <section
      aria-label="Container logs"
      className="flex min-w-0 flex-col gap-4"
    >
      <div className="flex flex-wrap items-center gap-2 rounded-xl border border-border bg-card p-3">
        <Input
          aria-label="Search loaded log lines"
          placeholder="Search loaded lines…"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          className="min-w-40 flex-1"
        />
        <Select
          searchable
          aria-label="Container"
          value={container}
          onValueChange={setContainer}
          className="w-full sm:w-48"
          options={[
            { value: "all", label: "All containers" },
            ...containers.map(([value, label]) => ({ value, label })),
          ]}
        />
        <Select
          aria-label="Output stream"
          value={stream}
          onValueChange={setStream}
          className="w-full sm:w-32"
          options={[
            { value: "all", label: "All output" },
            { value: "stdout", label: "stdout" },
            { value: "stderr", label: "stderr" },
          ]}
        />
        <Button
          variant={busy ? "default" : "outline"}
          disabled={
            !busy && (!Number.isSafeInteger(tail) || tail < 0 || tail > 1000)
          }
          onClick={() => {
            if (busy) {
              stop();
              setStatus("Paused");
            } else {
              start();
            }
          }}
        >
          {busy ? "Pause" : follow ? "Resume live" : "Read recent logs"}
        </Button>
        <CopyButton
          label="Copy visible logs"
          value={visible
            .map(
              (event) =>
                `${event.timestamp} ${event.container_name} ${event.stream} ${event.line}${event.truncated ? " [truncated]" : ""}`,
            )
            .join("\n")}
        />
      </div>
      <div className="flex flex-wrap items-center gap-4 text-sm text-muted-foreground">
        <label className="flex items-center gap-2">
          <Checkbox
            checked={wrap}
            onChange={(event) => setWrap(event.target.checked)}
          />
          Wrap lines
        </label>
        <label className="flex items-center gap-2">
          <Checkbox
            checked={follow}
            disabled={busy}
            onChange={(event) => setFollow(event.target.checked)}
          />
          Live on reload
        </label>
        <label className="flex items-center gap-2">
          Recent lines per container
          <Input
            aria-label="Recent lines per container"
            className="w-24"
            type="number"
            min={0}
            max={1000}
            value={tail}
            disabled={busy}
            onChange={(event) => setTail(Number(event.target.value))}
          />
        </label>
      </div>
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      <div className="flex items-center gap-3">
        <div className="flex min-w-0 flex-1 flex-wrap items-center justify-between gap-x-3 gap-y-1 text-xs text-muted-foreground">
          <span role="status">{status}</span>
          <span>
            {events.length > 0 &&
              replaceLogSnapshot.current &&
              "Previous read · "}
            {visible.length} shown · {events.length.toLocaleString()} retained ·{" "}
            {received.toLocaleString()} received
            {received > events.length && " · oldest lines discarded"}
          </span>
        </div>
        <Button
          size="sm"
          variant="outline"
          disabled={atEnd}
          className={atEnd ? "invisible" : undefined}
          onClick={() => {
            pinnedToEnd.current = true;
            setAtEnd(true);
            const output = outputRef.current;
            if (output) {
              output.scrollTop = output.scrollHeight;
              output.focus({ preventScroll: true });
            }
          }}
        >
          Jump to latest
        </Button>
      </div>
      {container !== "all" &&
        events.find((event) => event.container_id === container) && (
          <div className="flex min-w-0 items-center gap-2 text-xs text-muted-foreground">
            Selected container{" "}
            <LogSource
              event={events.find((event) => event.container_id === container)!}
            />
          </div>
        )}
      <div
        ref={outputRef}
        tabIndex={0}
        aria-label="Log output"
        onScroll={(event) => {
          const el = event.currentTarget;
          pinnedToEnd.current =
            el.scrollHeight - el.clientHeight - el.scrollTop < 32;
          setAtEnd(pinnedToEnd.current);
        }}
        className="h-[60vh] min-h-72 min-w-0 max-w-full overflow-auto overscroll-contain rounded-xl border border-border bg-card p-3 font-mono text-sm [overflow-anchor:none]"
      >
        {visible.length === 0 ? (
          <EmptyState
            embedded
            loading={busy && !events.length}
            title={
              events.length
                ? "No matching log lines"
                : status === "Connecting"
                  ? "Opening log stream"
                  : status === "Streaming"
                    ? "Waiting for container output"
                    : "No log output yet"
            }
            description={
              events.length
                ? "Try another search or filter."
                : "Containers may not have written output, or no workload is deployed."
            }
          />
        ) : (
          <LogRows
            events={visible}
            wrap={wrap}
            showSource={container === "all"}
            viewport={outputRef}
            pinned={pinnedToEnd}
            follow={follow}
          />
        )}
      </div>
    </section>
  );
}
