"use client";

import { Button } from "@/components/ui/button";
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

  useEffect(() => stop, []);
  useEffect(() => {
    if (follow && pinnedToEnd.current && outputRef.current)
      outputRef.current.scrollTop = outputRef.current.scrollHeight;
  }, [events, follow]);

  return (
    <section
      aria-label="Container logs"
      className="flex min-w-0 flex-col gap-4"
    >
      <div className="flex flex-wrap items-end gap-3 border-y border-border py-3">
        <label className="grid gap-1 text-xs font-medium">
          Tail per container
          <Input
            className="w-32"
            type="number"
            min={0}
            max={1000}
            value={tail}
            disabled={streaming}
            onChange={(event) => setTail(Number(event.target.value))}
          />
        </label>
        <label className="flex h-9 items-center gap-2 text-sm">
          <Checkbox
            checked={follow}
            disabled={streaming}
            onChange={(event) => setFollow(event.target.checked)}
          />{" "}
          Follow new output
        </label>
        <Button
          onClick={start}
          disabled={
            streaming || !Number.isSafeInteger(tail) || tail < 0 || tail > 1000
          }
        >
          {streaming
            ? "Reading logs"
            : follow
              ? "Open stream"
              : "Read recent logs"}
        </Button>
        {streaming ? (
          <Button variant="outline" onClick={stop}>
            Stop
          </Button>
        ) : null}
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
        <span>{events.length} / 1,000 buffered lines</span>
      </div>
      <div
        ref={outputRef}
        tabIndex={0}
        aria-label="Log output"
        onScroll={(event) => {
          const el = event.currentTarget;
          pinnedToEnd.current =
            el.scrollHeight - el.clientHeight - el.scrollTop < 32;
        }}
        className="h-[48vh] min-h-60 overflow-auto rounded-lg border border-border bg-[#100d17] p-4 font-mono text-xs text-[#f3effa]"
      >
        {events.length === 0 ? (
          <p className="text-muted-foreground">
            {!started
              ? "Open a stream to read container output."
              : streaming
                ? "Waiting for container output…"
                : "No log lines returned. There may be no deployed workload, or its containers have not written output."}
          </p>
        ) : (
          events.map((event) => (
            <div
              key={event.sequence}
              className="grid grid-cols-[5rem_minmax(0,1fr)_4rem] gap-x-3 gap-y-1 border-b border-border py-2 xl:grid-cols-[5rem_minmax(10rem,18rem)_4rem_minmax(0,1fr)]"
            >
              <span className="text-muted-foreground">
                {new Date(event.timestamp).toLocaleTimeString()}
              </span>
              <span className="min-w-0 whitespace-normal break-words text-primary">
                {event.service_name}
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
              <span className="col-span-3 min-w-0 whitespace-pre-wrap break-all xl:col-span-1">
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
