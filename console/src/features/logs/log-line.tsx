import { CopyButton } from "@/components/common/copy-button";
import { buttonVariants } from "@/components/ui/button";
import { Popover } from "@base-ui/react/popover";
import type { TransientLogEvent } from "@/lib/transient-logs";

export function LogSource({ event }: { event: TransientLogEvent }) {
  const name = event.container_name;
  return (
    <Popover.Root>
      <Popover.Trigger
        className={buttonVariants({
          variant: "ghost",
          size: "content",
          className:
            "min-w-0 max-w-full justify-start gap-1.5 truncate px-1 text-xs text-primary",
        })}
        aria-label={`Container details: ${name}`}
      >
        <span className="min-w-0 truncate">{event.service_name}</span>
        <span aria-hidden="true" className="shrink-0 text-muted-foreground/60">
          |
        </span>
        <span className="shrink-0 text-muted-foreground">
          {event.container_id.slice(0, 6)}
        </span>
      </Popover.Trigger>
      <Popover.Portal>
        <Popover.Positioner sideOffset={6} className="z-50">
          <Popover.Popup
            aria-label="Container details"
            className="w-80 max-w-[calc(100vw-2rem)] space-y-2 rounded-lg border border-border bg-popover p-3 font-sans text-popover-foreground shadow-xl"
          >
            <p className="text-xs text-muted-foreground">
              {event.service_name} · {event.slot}
            </p>
            <code className="block select-all break-all text-xs">{name}</code>
            <CopyButton value={name} label="Copy container name" />
            <p className="text-xs text-muted-foreground">Container ID</p>
            <code className="block select-all break-all text-xs">
              {event.container_id}
            </code>
            <CopyButton value={event.container_id} label="Copy container ID" />
          </Popover.Popup>
        </Popover.Positioner>
      </Popover.Portal>
    </Popover.Root>
  );
}

export function LogLine({
  event,
  wrap,
  showSource,
}: {
  event: TransientLogEvent;
  wrap: boolean;
  showSource: boolean;
}) {
  return (
    <div
      className={`grid min-w-0 grid-cols-[4.5rem_minmax(0,1fr)_3rem] items-start gap-x-2 xl:gap-x-3 gap-y-1 border-l-2 px-2 py-1 even:bg-muted/30 hover:bg-muted/50 ${event.stream === "stderr" ? "border-destructive/50" : "border-transparent"} ${showSource ? "xl:grid-cols-[6rem_12rem_3.5rem_minmax(0,1fr)]" : "xl:grid-cols-[6rem_3.5rem_minmax(0,1fr)]"}`}
    >
      <time
        dateTime={event.timestamp}
        title={event.timestamp}
        className="whitespace-nowrap text-xs tabular-nums leading-5 text-muted-foreground"
      >
        {new Date(event.timestamp).toLocaleTimeString(undefined, {
          hour12: false,
        })}
      </time>
      {showSource ? (
        <LogSource event={event} />
      ) : (
        <span className="xl:hidden" />
      )}
      <span
        className={`text-xs leading-5 ${event.stream === "stderr" ? "text-destructive" : "text-muted-foreground"}`}
      >
        {event.stream}
      </span>
      <span
        className={`col-span-3 min-w-0 leading-5 xl:col-span-1 ${wrap ? "whitespace-pre-wrap [overflow-wrap:anywhere]" : "overflow-x-auto whitespace-pre"}`}
      >
        {event.line || "\u00a0"}
        {event.truncated ? " [truncated]" : ""}
      </span>
    </div>
  );
}
