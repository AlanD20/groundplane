import type { components } from "@/lib/api.generated";

export type HostTerminalSession = components["schemas"]["HostTerminalSession"];

export type TerminalConnectionState = {
  session: HostTerminalSession;
  connected: boolean;
  error: string | null;
};

type ServerStatus = {
  type: "activity" | "terminated" | "output_truncated";
  session?: HostTerminalSession;
};

// One connection belongs to one shell, independently of which tab is visible.
export class TerminalConnection {
  private state: TerminalConnectionState;
  private socket: WebSocket | null = null;
  private sequence = 0;
  private retry: number | null = null;
  private disposed = false;
  private terminatePending = false;
  private listeners = new Set<() => void>();
  private outputListeners = new Set<(data: Uint8Array) => void>();

  constructor(session: HostTerminalSession) {
    this.state = { session, connected: false, error: null };
  }

  getSnapshot = () => this.state;

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  subscribeOutput = (listener: (data: Uint8Array) => void) => {
    // A fresh xterm viewport has no scrollback, including a StrictMode remount.
    // Its next connection must replay the retained buffer, not a stale cursor.
    if (this.outputListeners.size === 0 && this.socket === null)
      this.sequence = 0;
    this.outputListeners.add(listener);
    return () => this.outputListeners.delete(listener);
  };

  private update(patch: Partial<TerminalConnectionState>) {
    this.state = { ...this.state, ...patch };
    for (const listener of this.listeners) listener();
  }

  connect = () => {
    this.disposed = false;
    const current = this.state.session;
    if (this.socket || current.state !== "active") return;
    if (this.retry !== null) window.clearTimeout(this.retry);
    this.retry = null;
    const target = new URL(current.stream_path, window.location.href);
    target.protocol = target.protocol === "https:" ? "wss:" : "ws:";
    target.searchParams.set("after", String(this.sequence));
    const socket = new WebSocket(target);
    socket.binaryType = "arraybuffer";
    this.socket = socket;
    socket.onopen = () => {
      if (this.socket !== socket) return;
      this.update({ connected: true, error: null });
      if (this.terminatePending) this.send({ type: "terminate" });
    };
    socket.onmessage = (message) => {
      if (this.socket !== socket) return;
      if (message.data instanceof ArrayBuffer) {
        if (message.data.byteLength < 8) return;
        const sequence = Number(new DataView(message.data).getBigUint64(0));
        if (sequence <= this.sequence) return;
        this.sequence = sequence;
        this.output(new Uint8Array(message.data, 8));
        return;
      }
      try {
        const status = JSON.parse(String(message.data)) as ServerStatus;
        if (status.type === "output_truncated") {
          this.output(
            new TextEncoder().encode(
              "\r\n[earlier host terminal output was truncated]\r\n",
            ),
          );
        } else if (status.session?.id === current.id) {
          if (status.type === "terminated") this.terminatePending = false;
          this.update({
            session: status.session,
            connected: status.session.state === "active",
          });
        }
      } catch {
        this.update({
          error: "Host terminal returned an invalid status frame.",
        });
      }
    };
    socket.onerror = () => {
      if (this.socket === socket)
        this.update({ error: "Host terminal connection was interrupted." });
    };
    socket.onclose = (event) => {
      if (this.socket !== socket) return;
      this.socket = null;
      this.update({ connected: false });
      if (event.code === 1008) {
        this.terminatePending = false;
        this.update({
          session: {
            ...this.state.session,
            state: "terminated",
            reason: "session_unavailable",
          },
          error: "This shell has ended. Open a new terminal to continue.",
        });
      } else if (
        !this.disposed &&
        this.state.session.state === "active" &&
        Date.parse(this.state.session.expires_at) > Date.now()
      ) {
        this.retry = window.setTimeout(this.connect, 1_000);
      } else if (!this.disposed && this.state.session.state === "active") {
        this.update({
          error:
            "Connection lost. Reconnect to check whether this shell is still active.",
        });
      }
    };
  };

  private output(bytes: Uint8Array) {
    for (const listener of this.outputListeners) listener(bytes);
  }

  private send(message: {
    type: string;
    data?: string;
    columns?: number;
    rows?: number;
  }) {
    if (this.socket?.readyState === WebSocket.OPEN)
      this.socket.send(JSON.stringify(message));
  }

  sendInput = (data: string) => {
    if (this.state.session.state !== "active") return;
    for (let start = 0; start < data.length;) {
      let end = Math.min(start + 4_096, data.length);
      const final = data.charCodeAt(end - 1);
      if (end < data.length && final >= 0xd800 && final <= 0xdbff) end--;
      this.send({ type: "input", data: data.slice(start, end) });
      start = end;
    }
  };

  sendResize = (columns: number, rows: number) => {
    this.send({ type: "resize", columns, rows });
  };

  terminate = () => {
    if (this.state.session.state !== "active") return;
    this.terminatePending = true;
    if (this.socket?.readyState === WebSocket.OPEN)
      this.send({ type: "terminate" });
    else this.connect();
  };

  dispose = () => {
    this.disposed = true;
    if (this.retry !== null) window.clearTimeout(this.retry);
    this.retry = null;
    const socket = this.socket;
    this.socket = null;
    socket?.close();
    this.update({ connected: false });
  };
}
