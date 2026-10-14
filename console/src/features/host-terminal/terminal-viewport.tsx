import { FitAddon } from "@xterm/addon-fit";
import { Terminal } from "@xterm/xterm";
import "@xterm/xterm/css/xterm.css";
import { useEffect, useRef } from "react";
import { useHostTerminal } from "./terminal-provider";

export function TerminalViewport({
  id,
  active,
}: {
  id: string;
  active: boolean;
}) {
  const terminal = useHostTerminal(id);
  const connection = terminal.connection;
  const containerRef = useRef<HTMLDivElement>(null);
  const xtermRef = useRef<Terminal | null>(null);
  const fitRef = useRef<FitAddon | null>(null);

  useEffect(() => {
    const container = containerRef.current;
    if (!container || !connection) return;
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
    const input = xterm.onData(connection.sendInput);
    const resize = xterm.onResize(({ cols, rows }) =>
      connection.sendResize(
        Math.max(20, Math.min(500, cols)),
        Math.max(5, Math.min(300, rows)),
      ),
    );
    const unsubscribe = connection.subscribeOutput((value) =>
      xterm.write(value),
    );
    const observer = new ResizeObserver(() => {
      if (container.offsetParent !== null) fit.fit();
    });
    observer.observe(container);
    xtermRef.current = xterm;
    fitRef.current = fit;
    connection.connect();
    return () => {
      observer.disconnect();
      unsubscribe();
      input.dispose();
      resize.dispose();
      xterm.dispose();
      connection.dispose();
      xtermRef.current = null;
      fitRef.current = null;
    };
  }, [connection]);

  useEffect(() => {
    if (!active || !terminal.visible) return;
    const frame = requestAnimationFrame(() => {
      fitRef.current?.fit();
      if (document.activeElement?.getAttribute("role") !== "tab")
        xtermRef.current?.focus();
    });
    return () => cancelAnimationFrame(frame);
  }, [active, terminal.visible, terminal.maximized, terminal.height]);

  useEffect(() => {
    if (xtermRef.current)
      xtermRef.current.options.disableStdin =
        !terminal.connected || terminal.session?.state !== "active";
    if (terminal.connected && active && terminal.visible) fitRef.current?.fit();
  }, [terminal.connected, terminal.session?.state, active, terminal.visible]);

  return (
    <div
      id={`terminal-panel-${id}`}
      role="tabpanel"
      aria-labelledby={`terminal-tab-${id}`}
      ref={containerRef}
      hidden={!active}
      className="min-h-0 flex-1 px-2 py-1"
    />
  );
}
