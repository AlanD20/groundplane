import { useVirtualizer } from "@tanstack/react-virtual";
import { useEffect, useMemo, type RefObject } from "react";
import type { TransientLogEvent } from "@/lib/transient-logs";
import { LogLine } from "./log-line";

export function LogRows({
  events,
  wrap,
  showSource,
  viewport,
  pinned,
  follow,
}: {
  events: TransientLogEvent[];
  wrap: boolean;
  showSource: boolean;
  viewport: RefObject<HTMLDivElement | null>;
  pinned: RefObject<boolean>;
  follow: boolean;
}) {
  const rows = useVirtualizer({
    count: events.length,
    getScrollElement: () => viewport.current,
    getItemKey: (index) => events[index].sequence,
    estimateSize: () => 28,
    overscan: 8,
    anchorTo: "end",
  });
  const longest = useMemo(
    () =>
      wrap
        ? ""
        : events.reduce(
            (line, event) =>
              event.line.length > line.length ? event.line : line,
            "",
          ),
    [events, wrap],
  );
  useEffect(() => {
    rows.measure();
  }, [wrap, showSource, rows]);
  useEffect(() => {
    if (follow && pinned.current && events.length)
      rows.scrollToIndex(events.length - 1, { align: "end" });
  }, [events, follow, pinned, rows]);
  return (
    <div
      className={wrap ? "relative min-w-0" : "relative w-max min-w-full"}
      style={{ height: rows.getTotalSize() }}
    >
      {!wrap && (
        <div aria-hidden className="invisible h-0 whitespace-pre pl-[30rem]">
          {longest}
        </div>
      )}
      {rows.getVirtualItems().map((row) => (
        <div
          key={row.key}
          data-index={row.index}
          ref={rows.measureElement}
          className="absolute left-0 top-0 w-full"
          style={{ transform: `translateY(${row.start}px)` }}
        >
          <LogLine
            event={events[row.index]}
            wrap={wrap}
            showSource={showSource}
            alternate={events[row.index].sequence % 2 === 1}
          />
        </div>
      ))}
    </div>
  );
}
