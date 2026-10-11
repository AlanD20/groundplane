import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { FitAddon } from "@xterm/addon-fit";
import { Terminal } from "@xterm/xterm";
import "@xterm/xterm/css/xterm.css";
import {
  ChevronsDownUp,
  ChevronsUpDown,
  GripHorizontal,
  Minus,
  SquareTerminal,
  X,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { useHostTerminal } from "./terminal-provider";

export function HostTerminalPanel() {
  const terminal = useHostTerminal();
  const [confirmClose, setConfirmClose] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);
  const xtermRef = useRef<Terminal | null>(null);
  const fitRef = useRef<FitAddon | null>(null);
  const renderPanel = Boolean(
    terminal.session || terminal.opening || terminal.error,
  );

  useEffect(() => {
    const container = containerRef.current;
    if (!container || xtermRef.current) return;
    const xterm = new Terminal({
      cursorBlink: true,
      convertEol: false,
      scrollback: 5_000,
      fontFamily: '"IBM Plex Mono", ui-monospace, monospace',
      fontSize: 13,
      lineHeight: 1.25,
      theme: {
        background: "#111318",
        foreground: "#e8eaf0",
        cursor: "#f5a524",
        selectionBackground: "#46505f99",
      },
    });
    const fit = new FitAddon();
    xterm.loadAddon(fit);
    xterm.open(container);
    fit.fit();
    const data = xterm.onData(terminal.sendInput);
    const resize = xterm.onResize(({ cols, rows }) =>
      terminal.sendResize(cols, rows),
    );
    const unsubscribe = terminal.subscribeOutput((value) => xterm.write(value));
    const observer = new ResizeObserver(() => {
      if (container.offsetParent !== null) fit.fit();
    });
    observer.observe(container);
    xtermRef.current = xterm;
    fitRef.current = fit;
    return () => {
      observer.disconnect();
      unsubscribe();
      data.dispose();
      resize.dispose();
      xterm.dispose();
      xtermRef.current = null;
      fitRef.current = null;
    };
  }, [
    terminal.sendInput,
    terminal.sendResize,
    terminal.subscribeOutput,
    terminal.session?.id,
    renderPanel,
  ]);

  useEffect(() => {
    if (!terminal.visible) return;
    const frame = requestAnimationFrame(() => {
      fitRef.current?.fit();
      xtermRef.current?.focus();
    });
    return () => cancelAnimationFrame(frame);
  }, [terminal.visible, terminal.maximized, terminal.height]);

  useEffect(() => {
    if (xtermRef.current) {
      xtermRef.current.options.disableStdin =
        !terminal.connected || terminal.session?.state !== "active";
    }
  }, [terminal.connected, terminal.session?.state]);

  if (!renderPanel) return null;

  const status =
    terminal.session?.state === "active"
      ? terminal.connected
        ? "Active"
        : "Reconnecting"
      : terminal.session?.reason
        ? formatReason(terminal.session.reason)
        : "Closed";

  return (
    <>
      <section
        aria-label="Host terminal"
        className={cn(
          "flex flex-col border-t border-border bg-[#111318] text-zinc-100 shadow-[0_-12px_36px_rgba(0,0,0,0.2)]",
          !terminal.visible && "hidden",
          terminal.maximized && "fixed inset-0 z-50 border-0",
        )}
        style={terminal.maximized ? undefined : { height: terminal.height }}
      >
        {!terminal.maximized ? (
          <div
            role="separator"
            aria-label="Resize host terminal"
            aria-orientation="horizontal"
            tabIndex={0}
            className="group flex h-2 cursor-row-resize touch-none items-center justify-center bg-zinc-900 outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-amber-400"
            onPointerDown={(event) => {
              event.currentTarget.setPointerCapture(event.pointerId);
              const startY = event.clientY;
              const startHeight = terminal.height;
              const move = (moveEvent: PointerEvent) =>
                terminal.setHeight(startHeight + startY - moveEvent.clientY);
              const stop = () => {
                window.removeEventListener("pointermove", move);
                window.removeEventListener("pointerup", stop);
              };
              window.addEventListener("pointermove", move);
              window.addEventListener("pointerup", stop);
            }}
            onKeyDown={(event) => {
              if (event.key === "ArrowUp" || event.key === "ArrowDown") {
                event.preventDefault();
                terminal.setHeight(
                  terminal.height + (event.key === "ArrowUp" ? 24 : -24),
                );
              }
            }}
          >
            <GripHorizontal className="size-4 text-zinc-500 group-hover:text-zinc-300" />
          </div>
        ) : null}
        <div className="flex h-11 items-center gap-3 border-b border-white/10 px-3 text-xs">
          <SquareTerminal className="size-4 text-amber-400" aria-hidden />
          <span className="font-medium">Host terminal</span>
          {terminal.session ? (
            <>
              <span className="rounded bg-white/10 px-2 py-1 font-mono text-[11px]">
                {terminal.session.user}@{terminal.session.host}
              </span>
              <span className="flex items-center gap-1.5 text-zinc-400">
                <span
                  className={cn(
                    "size-2 rounded-full",
                    terminal.connected ? "bg-emerald-400" : "bg-amber-400",
                  )}
                />
                {status}
              </span>
              {terminal.session.state === "active" ? (
                <span className="ml-auto text-zinc-400">
                  Input idle limit ·{" "}
                  {formatRemaining(terminal.remainingSeconds)} remaining
                </span>
              ) : (
                <span className="ml-auto text-zinc-400">{status}</span>
              )}
            </>
          ) : (
            <span className="text-zinc-400">Opening root shell…</span>
          )}
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            className="text-zinc-400 hover:bg-white/10 hover:text-white"
            aria-label={
              terminal.maximized
                ? "Restore terminal panel"
                : "Maximize terminal in this tab"
            }
            onClick={() => terminal.setMaximized(!terminal.maximized)}
          >
            {terminal.maximized ? <ChevronsDownUp /> : <ChevronsUpDown />}
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            className="text-zinc-400 hover:bg-white/10 hover:text-white"
            aria-label="Hide terminal"
            onClick={terminal.hide}
          >
            <Minus />
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            className="text-zinc-400 hover:bg-white/10 hover:text-white"
            aria-label="Close terminal"
            onClick={() => {
              if (terminal.session?.state === "active") setConfirmClose(true);
              else terminal.hide();
            }}
          >
            <X />
          </Button>
        </div>
        {terminal.error ? (
          <div
            role="alert"
            className="border-b border-red-400/20 bg-red-400/10 px-4 py-2 text-xs text-red-200"
          >
            {terminal.error}
          </div>
        ) : null}
        <div ref={containerRef} className="min-h-0 flex-1 px-2 py-1" />
      </section>

      <Dialog open={confirmClose} onOpenChange={setConfirmClose}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Close host terminal?</DialogTitle>
            <DialogDescription>
              Keep it running in the background for the same browser session, or
              terminate the root shell and every process it owns.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                terminal.hide();
                setConfirmClose(false);
              }}
            >
              Keep in background
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                terminal.terminate();
                setConfirmClose(false);
              }}
            >
              Terminate session
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

function formatRemaining(seconds: number) {
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes}:${String(remainder).padStart(2, "0")}`;
}

function formatReason(reason: string) {
  return reason
    .replaceAll("_", " ")
    .replace(/^./, (value) => value.toUpperCase());
}
