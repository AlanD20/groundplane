import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import {
  ContextMenu,
  ContextMenuTrigger,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
} from "@/components/ui/context-menu";
import { cn } from "@/lib/utils";
import { Input } from "@/components/ui/input";
import {
  ChevronsDownUp,
  ChevronsUpDown,
  GripHorizontal,
  Minus,
  SquareTerminal,
  X,
  Plus,
  Pencil,
  Square,
} from "lucide-react";
import { useEffect, useState } from "react";
import { useHostTerminal } from "./terminal-provider";
import { TerminalViewport } from "./terminal-viewport";

export function HostTerminalPanel() {
  const terminal = useHostTerminal();
  const [confirmClose, setConfirmClose] = useState<string | null>(null);
  const [renaming, setRenaming] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [closeAfterTerminate, setCloseAfterTerminate] = useState(false);
  const [pendingClose, setPendingClose] = useState<string[]>([]);
  const requestClose = (id: string) => {
    const tab = terminal.tabs.find((item) => item.id === id);
    if (tab?.connection.getSnapshot().session.state === "active") {
      setCloseAfterTerminate(true);
      setConfirmClose(id);
    } else terminal.closeTab(id);
  };
  useEffect(() => {
    const releases = pendingClose.map((id) => {
      const tab = terminal.tabs.find((item) => item.id === id);
      if (!tab) return () => {};
      let unsubscribe = () => {};
      const finish = () => {
        if (tab.connection.getSnapshot().session.state !== "active") {
          unsubscribe();
          terminal.closeTab(id);
          setPendingClose((current) => current.filter((item) => item !== id));
        }
      };
      unsubscribe = tab.connection.subscribe(finish);
      finish();
      return unsubscribe;
    });
    return () => {
      for (const release of releases) release();
    };
  }, [pendingClose, terminal.tabs, terminal.closeTab]);
  const renderPanel = Boolean(
    terminal.tabs.length || terminal.opening || terminal.openError,
  );

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
        </div>
        <div
          role="tablist"
          aria-label="Host shells"
          className="flex shrink-0 items-center gap-1 overflow-x-auto border-b border-white/10 px-2 py-1"
        >
          {terminal.tabs.map((tab, index) => (
            <ContextMenu key={tab.id}>
              <ContextMenuTrigger
                className={cn(
                  "flex shrink-0 items-center rounded-md",
                  terminal.activeId === tab.id && "bg-white/10",
                )}
              >
                <Button
                  id={`terminal-tab-${tab.id}`}
                  role="tab"
                  aria-selected={terminal.activeId === tab.id}
                  aria-controls={`terminal-panel-${tab.id}`}
                  tabIndex={terminal.activeId === tab.id ? 0 : -1}
                  variant="ghost"
                  size="sm"
                  className={cn(
                    "shrink-0 text-zinc-400 hover:bg-white/10 hover:text-white",
                    terminal.activeId === tab.id && "bg-white/10 text-white",
                  )}
                  onClick={() => terminal.selectTab(tab.id)}
                  onPointerDown={(event) => {
                    // Pointer selection returns directly to shell input; arrow-key
                    // navigation keeps focus on the tablist.
                    if (event.button === 0) event.preventDefault();
                  }}
                  onKeyDown={(event) => {
                    if (event.key === "Delete") {
                      event.preventDefault();
                      requestClose(tab.id);
                      return;
                    }
                    const next =
                      event.key === "ArrowRight"
                        ? (index + 1) % terminal.tabs.length
                        : event.key === "ArrowLeft"
                          ? (index + terminal.tabs.length - 1) %
                            terminal.tabs.length
                          : event.key === "Home"
                            ? 0
                            : event.key === "End"
                              ? terminal.tabs.length - 1
                              : -1;
                    if (next >= 0) {
                      event.preventDefault();
                      terminal.selectTab(terminal.tabs[next].id);
                      document
                        .getElementById(
                          `terminal-tab-${terminal.tabs[next].id}`,
                        )
                        ?.focus();
                    }
                  }}
                >
                  <span className="max-w-48 truncate">
                    {tab.name}
                    {pendingClose.includes(tab.id) ? " · Closing…" : ""}
                  </span>
                </Button>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon-sm"
                  aria-label={`Close ${tab.name}`}
                  className="mr-1 shrink-0 text-zinc-400 hover:bg-white/10 hover:text-white"
                  onClick={() => requestClose(tab.id)}
                >
                  <X className="size-3.5" />
                </Button>
              </ContextMenuTrigger>
              <ContextMenuContent>
                <ContextMenuItem
                  onClick={() => {
                    setName(tab.name);
                    setRenaming(tab.id);
                  }}
                >
                  <Pencil />
                  Rename
                </ContextMenuItem>
                <ContextMenuItem onClick={() => requestClose(tab.id)}>
                  <X />
                  Close tab
                </ContextMenuItem>
                <ContextMenuSeparator />
                <ContextMenuItem
                  variant="destructive"
                  onClick={() => {
                    setCloseAfterTerminate(false);
                    setConfirmClose(tab.id);
                  }}
                >
                  <Square />
                  Terminate session
                </ContextMenuItem>
              </ContextMenuContent>
            </ContextMenu>
          ))}
          <Button
            type="button"
            variant="ghost"
            size="icon-sm"
            aria-label="New terminal tab"
            className="shrink-0 text-zinc-400 hover:bg-white/10 hover:text-white"
            disabled={terminal.opening}
            onClick={() => void terminal.newTab()}
          >
            <Plus />
          </Button>
        </div>
        {terminal.error || terminal.openError ? (
          <div
            role="alert"
            className="border-b border-red-400/20 bg-red-400/10 px-4 py-2 text-xs text-red-200"
          >
            {terminal.openError ?? terminal.error}
          </div>
        ) : null}
        {terminal.tabs.map((tab) => (
          <TerminalViewport
            key={tab.id}
            id={tab.id}
            active={terminal.activeId === tab.id}
          />
        ))}
      </section>

      <Dialog
        open={confirmClose !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmClose(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {closeAfterTerminate ? "Close" : "Terminate"}{" "}
              {terminal.tabs.find((tab) => tab.id === confirmClose)?.name ??
                "terminal"}
              ?
            </DialogTitle>
            <DialogDescription>
              This terminates this shell and its running processes.
              {closeAfterTerminate
                ? " The tab closes once the session ends."
                : " The tab and its output remain available."}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setConfirmClose(null);
              }}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                if (closeAfterTerminate && confirmClose)
                  setPendingClose((current) =>
                    current.includes(confirmClose)
                      ? current
                      : [...current, confirmClose],
                  );
                terminal.tabs
                  .find((tab) => tab.id === confirmClose)
                  ?.connection.terminate();
                setConfirmClose(null);
              }}
            >
              Terminate session
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      <Dialog
        open={renaming !== null}
        onOpenChange={(open) => {
          if (!open) setRenaming(null);
        }}
      >
        <DialogContent>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              if (renaming && name.trim()) {
                terminal.renameTab(renaming, name);
                setRenaming(null);
              }
            }}
          >
            <DialogHeader>
              <DialogTitle>Rename terminal tab</DialogTitle>
              <DialogDescription>
                This label only identifies the tab.
              </DialogDescription>
            </DialogHeader>
            <Input
              aria-label="Tab name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              maxLength={40}
              required
              autoFocus
            />
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => setRenaming(null)}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={!name.trim()}>
                Rename
              </Button>
            </DialogFooter>
          </form>
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
