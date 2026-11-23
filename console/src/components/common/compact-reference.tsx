import { useState } from "react";
import { CopyButton } from "./copy-button";
import { Button } from "@/components/ui/button";
import { Popover } from "@base-ui/react/popover";
import { X } from "lucide-react";

/** Short labels are presentation only; the complete reference remains selectable. */
export function CompactReference({
  value,
  label = "ID",
  short,
  hideLabel = false,
}: {
  value: string;
  label?: string;
  short?: string;
  hideLabel?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const abbreviated =
    short ??
    (value.length > 24 ? `${value.slice(0, 8)}…${value.slice(-8)}` : value);
  return (
    <span className="inline-flex min-w-0 max-w-full items-center gap-1 font-sans text-xs">
      <Popover.Root open={open} onOpenChange={setOpen}>
        <Popover.Trigger
          type="button"
          aria-label={`Show full ${label}`}
          title={open ? undefined : value}
          className="min-w-0 truncate rounded px-1 py-1 text-muted-foreground hover:bg-muted hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring"
        >
          {!hideLabel && <>{label} </>}
          <span className="font-mono">{abbreviated}</span>
        </Popover.Trigger>
        <Popover.Portal>
          <Popover.Positioner align="start" sideOffset={6} className="z-[60]">
            <Popover.Popup className="w-80 max-w-[calc(100vw-2rem)] space-y-2 rounded-lg border border-border bg-popover p-3 text-popover-foreground shadow-xl">
              <div className="flex items-center justify-between gap-2">
                <Popover.Title className="text-xs font-medium">
                  {label}
                </Popover.Title>
                <div className="flex shrink-0 items-center gap-1">
                  <CopyButton value={value} label={`Copy ${label}`} />
                  <Popover.Close
                    render={
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        aria-label="Close reference"
                      />
                    }
                  >
                    <X className="size-3.5" />
                  </Popover.Close>
                </div>
              </div>
              <code className="block select-all break-all rounded-md bg-muted px-2 py-1.5 text-xs">
                {value}
              </code>
            </Popover.Popup>
          </Popover.Positioner>
        </Popover.Portal>
      </Popover.Root>
      <CopyButton
        value={value}
        label={`Copy ${label}`}
        iconOnly
        className="shrink-0"
      />
    </span>
  );
}
