"use client";

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
import { useEffect, useRef, useState } from "react";

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
              Read output from deployed containers. Reopen after a deployment to
              select the new containers.
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
  const [error, setError] = useState<string | null>(null);
  const [streaming, setStreaming] = useState(false);
  const [started, setStarted] = useState(false);
  const outputRef = useRef<HTMLDivElement>(null);
  const pinnedToEnd = useRef(true);
  const abortRef = useRef<AbortController | null>(null);
  const pendingEvents = useRef<TransientLogEvent[]>([]);
  const flushTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const flushEvents = () => {
    if (flushTimer.current !== null) {
      clearTimeout(flushTimer.current);
      flushTimer.current = null;
    }
    const batch = pendingEvents.current;
    pendingEvents.current = [];
    if (batch.length > 0) {
      setEvents((current) => [...current, ...batch].slice(-1000));
    }
  };

  const stop = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    flushEvents();
    setStreaming(false);
  };

  const start = () => {
    stop();
    const controller = new AbortController();
    abortRef.current = controller;
    setEvents([]);
    setStarted(true);
    pinnedToEnd.current = true;
    setAtEnd(true);
    setError(null);
    setStreaming(true);
    void store
      .watchLogs(
        target,
        { tail, follow, signal: controller.signal },
        (event) => {
          if (controller.signal.aborted) return;
          pendingEvents.current.push(event);
          if (flushTimer.current === null) {
            flushTimer.current = setTimeout(flushEvents, 50);
          }
        },
      )
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          setError(
            reason instanceof Error ? reason.message : "Log stream failed",
          );
        }
      })
      .finally(() => {
        if (abortRef.current === controller) {
          flushEvents();
          abortRef.current = null;
          setStreaming(false);
        }
      });
  };

  useEffect(() => {
    setContainer("all");
    start();
    return stop;
    // A new target owns a new stream; filters never restart it.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target.kind, target.id]);
  useEffect(() => {
    if (follow && pinnedToEnd.current && outputRef.current)
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
  }, [events, follow]);

  const visible = events.filter(
    (event) =>
      (container === "all" || event.container_id === container) &&
      (stream === "all" || event.stream === stream) &&
      event.line.toLowerCase().includes(query.toLowerCase()),
  );
  const containers = [
    ...new Map(
      events.map((event) => [event.container_id, event.container_name]),
    ).entries(),
  ];
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
          variant={streaming ? "default" : "outline"}
          disabled={
            !streaming &&
            (!Number.isSafeInteger(tail) || tail < 0 || tail > 1000)
          }
          onClick={streaming ? stop : start}
        >
          {streaming ? "Pause live" : "Resume / reload"}
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
            disabled={streaming}
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
            disabled={streaming}
            onChange={(event) => setTail(Number(event.target.value))}
          />
        </label>
      </div>
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span role="status">
          {streaming
            ? "Streaming"
            : error
              ? "Disconnected"
              : started
                ? "Stream closed"
                : "Ready"}
        </span>
        <span>
          {visible.length} shown · {events.length} / 1,000 loaded lines
        </span>
      </div>
      {!atEnd && (
        <Button
          size="sm"
          variant="outline"
          className="self-end"
          onClick={() => {
            pinnedToEnd.current = true;
            setAtEnd(true);
            if (outputRef.current)
              outputRef.current.scrollTop = outputRef.current.scrollHeight;
          }}
        >
          Jump to latest
        </Button>
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
        className="h-[60vh] min-h-72 overflow-auto rounded-xl border border-border bg-card p-3 font-mono text-sm"
      >
        {visible.length === 0 ? (
          <p className="text-muted-foreground">
            {events.length > 0
              ? "No loaded lines match these filters."
              : !started
                ? "Open a stream to read container output."
                : streaming
                  ? "Waiting for container output…"
                  : "No log lines returned. There may be no deployed workload, or its containers have not written output."}
          </p>
        ) : (
          visible.map((event) => (
            <div
              key={event.sequence}
              className="grid grid-cols-[5rem_minmax(0,1fr)_4rem] gap-x-3 gap-y-1 rounded px-2 py-1 hover:bg-muted/50 xl:grid-cols-[5rem_10rem_4rem_minmax(0,1fr)]"
            >
              <span className="text-muted-foreground">
                {new Date(event.timestamp).toLocaleTimeString()}
              </span>
              <span className="min-w-0 whitespace-normal break-words text-primary">
                {event.container_name}
              </span>
              <span
                className={
                  event.stream === "stderr"
                    ? "text-destructive"
                    : "text-success"
                }
              >
                {event.stream}
              </span>
              <span
                className={`col-span-3 min-w-0 xl:col-span-1 ${wrap ? "whitespace-pre-wrap [overflow-wrap:anywhere]" : "whitespace-pre"}`}
              >
                {event.line}
                {event.truncated ? " [truncated]" : ""}
              </span>
            </div>
          ))
        )}
      </div>
    </section>
  );
}
