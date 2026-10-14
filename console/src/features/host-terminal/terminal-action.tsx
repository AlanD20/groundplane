import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { SquareTerminal } from "lucide-react";
import { useHostTerminal } from "./terminal-provider";

export function HostTerminalAction() {
  const terminal = useHostTerminal();
  const active = terminal.session?.state === "active";
  return (
    <Button
      type="button"
      variant="outline"
      size="sm"
      className="gap-1.5"
      disabled={terminal.opening}
      aria-label={
        active && !terminal.visible
          ? "Show active host terminal"
          : "Open host terminal"
      }
      onClick={() => {
        void terminal.open();
      }}
    >
      <span className="relative">
        <SquareTerminal className="size-4" aria-hidden />
        {active ? (
          <span
            className={cn(
              "absolute -right-1 -top-1 size-2 rounded-full border border-background",
              terminal.connected ? "bg-success" : "bg-warning",
            )}
          />
        ) : null}
      </span>
      <span className="hidden sm:inline">Terminal</span>
      {active && !terminal.visible ? (
        <span className="hidden text-[10px] text-muted-foreground xl:inline">
          Background
        </span>
      ) : null}
    </Button>
  );
}
