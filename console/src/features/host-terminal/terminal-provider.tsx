import type { components } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import { newULID } from "@/lib/utils";
import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

export type HostTerminalSession = components["schemas"]["HostTerminalSession"];

type TerminalContextValue = {
  session: HostTerminalSession | null;
  visible: boolean;
  opening: boolean;
  connected: boolean;
  maximized: boolean;
  height: number;
  error: string | null;
  remainingSeconds: number;
  open: () => Promise<void>;
  show: () => void;
  hide: () => void;
  terminate: () => void;
  sendInput: (data: string) => void;
  sendResize: (columns: number, rows: number) => void;
  subscribeOutput: (listener: (data: Uint8Array) => void) => () => void;
  setMaximized: (value: boolean) => void;
  setHeight: (value: number) => void;
};

const storageKey = "groundplane-host-terminal-session";
const pendingOpenStorageKey = "groundplane-host-terminal-open";
const TerminalContext = createContext<TerminalContextValue | null>(null);

type PendingOpen = {
  idempotencyKey: string;
  columns: number;
  rows: number;
};

type ServerStatus = {
  type: "activity" | "terminated" | "output_truncated";
  session?: HostTerminalSession;
};

export function HostTerminalProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [session, setSession] = useState<HostTerminalSession | null>(() =>
    restoreSession(),
  );
  const [visible, setVisible] = useState(false);
  const [opening, setOpening] = useState(false);
  const [connected, setConnected] = useState(false);
  const [maximized, setMaximized] = useState(false);
  const [height, setHeightState] = useState(360);
  const [error, setError] = useState<string | null>(null);
  const [now, setNow] = useState(Date.now());
  const openingRef = useRef(false);
  const pendingOpenRef = useRef<PendingOpen | null>(restorePendingOpen());
  const socketRef = useRef<WebSocket | null>(null);
  const sessionRef = useRef(session);
  const sequenceRef = useRef(0);
  const retryRef = useRef<number | null>(null);
  const activeRef = useRef(true);
  const terminatePendingRef = useRef(false);
  const outputListeners = useRef(new Set<(data: Uint8Array) => void>());

  useEffect(() => {
    sessionRef.current = session;
    persistSession(session);
  }, [session]);

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, []);

  const connect = useCallback((current: HostTerminalSession) => {
    if (socketRef.current || current.state !== "active") return;
    const target = new URL(current.stream_path, window.location.href);
    target.protocol = target.protocol === "https:" ? "wss:" : "ws:";
    target.searchParams.set("after", String(sequenceRef.current));
    const socket = new WebSocket(target);
    socket.binaryType = "arraybuffer";
    socketRef.current = socket;
    socket.onopen = () => {
      if (socketRef.current !== socket) return;
      setConnected(true);
      setError(null);
      if (terminatePendingRef.current) {
        socket.send(JSON.stringify({ type: "terminate" }));
      }
    };
    socket.onmessage = (message) => {
      if (socketRef.current !== socket) return;
      if (message.data instanceof ArrayBuffer) {
        if (message.data.byteLength < 8) return;
        const view = new DataView(message.data);
        const sequence = Number(view.getBigUint64(0));
        if (sequence <= sequenceRef.current) return;
        sequenceRef.current = sequence;
        const bytes = new Uint8Array(message.data, 8);
        for (const listener of outputListeners.current) listener(bytes);
        return;
      }
      try {
        const status = JSON.parse(String(message.data)) as ServerStatus;
        if (status.type === "output_truncated") {
          const notice = new TextEncoder().encode(
            "\r\n[earlier host terminal output was truncated]\r\n",
          );
          for (const listener of outputListeners.current) listener(notice);
        } else if (status.session) {
          sessionRef.current = status.session;
          setSession(status.session);
          if (status.type === "terminated") {
            terminatePendingRef.current = false;
            setConnected(false);
          }
        }
      } catch {
        setError("Host terminal returned an invalid status frame.");
      }
    };
    socket.onerror = () => {
      if (socketRef.current === socket) setError("Host terminal connection was interrupted.");
    };
    socket.onclose = (event) => {
      if (socketRef.current !== socket) return;
      socketRef.current = null;
      setConnected(false);
      const latest = sessionRef.current;
      if (event.code === 1008 && latest?.id === current.id) {
        const ended: HostTerminalSession = { ...latest, state: "terminated", reason: "session_unavailable" };
        sessionRef.current = ended;
        setSession(ended);
        terminatePendingRef.current = false;
        setError("This host terminal session has ended. Open a new terminal to continue.");
        return;
      }
      if (
        latest?.state === "active" &&
        activeRef.current &&
        Date.parse(latest.expires_at) > Date.now()
      ) {
        retryRef.current = window.setTimeout(() => connect(latest), 1_000);
      }
    };
  }, []);

  useEffect(() => {
    if (session?.state === "active") connect(session);
    return () => {
      if (retryRef.current !== null) window.clearTimeout(retryRef.current);
    };
  }, [connect, session?.id, session?.state]);

  useEffect(() => {
    activeRef.current = true;
    return () => {
      activeRef.current = false;
      if (retryRef.current !== null) window.clearTimeout(retryRef.current);
      socketRef.current?.close();
    };
  }, []);

  const open = useCallback(async () => {
    setVisible(true);
    setMaximized(false);
    const current = sessionRef.current;
    if (current?.state === "active" && connected && Date.parse(current.expires_at) > Date.now()) {
      return;
    }
    if (openingRef.current) return;
    openingRef.current = true;
    setOpening(true);
    setError(null);
    // An explicit Open can recover from an expired session or Controller
    // restart. Automatic WebSocket reconnect never creates a new root shell.
    sessionRef.current = null;
    if (retryRef.current !== null) window.clearTimeout(retryRef.current);
    const previousSocket = socketRef.current;
    socketRef.current = null;
    setConnected(false);
    previousSocket?.close();
    terminatePendingRef.current = false;
    const pending = pendingOpenRef.current ?? {
      idempotencyKey: newULID(),
      columns: 100,
      rows: 28,
    };
    pendingOpenRef.current = pending;
    persistPendingOpen(pending);
    try {
      const created = await controllerRequest<HostTerminalSession>(
        "/host/terminal",
        201,
        {
          method: "POST",
          body: { columns: pending.columns, rows: pending.rows },
          idempotencyKey: pending.idempotencyKey,
        },
      );
      pendingOpenRef.current = null;
      persistPendingOpen(null);
      if (created.id !== current?.id) sequenceRef.current = 0;
      sessionRef.current = created;
      setSession(created);
      connect(created);
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "Unable to open host terminal.",
      );
    } finally {
      openingRef.current = false;
      setOpening(false);
    }
  }, [connect, connected]);

  const terminate = useCallback(() => {
    terminatePendingRef.current = true;
    const current = sessionRef.current;
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: "terminate" }));
    } else if (current?.state === "active") {
      connect(current);
    }
  }, [connect]);

  const sendInput = useCallback((data: string) => {
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN && data.length > 0) {
      for (let start = 0; start < data.length;) {
        let end = Math.min(start + 4_096, data.length);
        const final = data.charCodeAt(end - 1);
        if (end < data.length && final >= 0xd800 && final <= 0xdbff) end--;
        socket.send(
          JSON.stringify({ type: "input", data: data.slice(start, end) }),
        );
        start = end;
      }
    }
  }, []);

  const sendResize = useCallback((columns: number, rows: number) => {
    const socket = socketRef.current;
    if (socket?.readyState === WebSocket.OPEN) {
      socket.send(JSON.stringify({ type: "resize", columns, rows }));
    }
  }, []);

  const subscribeOutput = useCallback(
    (listener: (data: Uint8Array) => void) => {
      outputListeners.current.add(listener);
      return () => outputListeners.current.delete(listener);
    },
    [],
  );

  const setHeight = useCallback((value: number) => {
    setHeightState(Math.max(220, Math.min(value, window.innerHeight * 0.75)));
  }, []);

  const remainingSeconds =
    session?.state === "active"
      ? Math.max(0, Math.ceil((Date.parse(session.expires_at) - now) / 1_000))
      : 0;

  const value = useMemo<TerminalContextValue>(
    () => ({
      session,
      visible,
      opening,
      connected,
      maximized,
      height,
      error,
      remainingSeconds,
      open,
      show: () => setVisible(true),
      hide: () => {
        setVisible(false);
        setMaximized(false);
      },
      terminate,
      sendInput,
      sendResize,
      subscribeOutput,
      setMaximized,
      setHeight,
    }),
    [
      session,
      visible,
      opening,
      connected,
      maximized,
      height,
      error,
      remainingSeconds,
      open,
      terminate,
      sendInput,
      sendResize,
      subscribeOutput,
      setHeight,
    ],
  );
  return (
    <TerminalContext.Provider value={value}>
      {children}
    </TerminalContext.Provider>
  );
}

export function useHostTerminal() {
  const value = useContext(TerminalContext);
  if (!value)
    throw new Error("useHostTerminal must be used within HostTerminalProvider");
  return value;
}

function restoreSession(): HostTerminalSession | null {
  try {
    const raw = sessionStorage.getItem(storageKey);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as HostTerminalSession;
    if (
      !parsed.id ||
      parsed.state !== "active" ||
      Date.parse(parsed.expires_at) <= Date.now()
    ) {
      sessionStorage.removeItem(storageKey);
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function persistSession(session: HostTerminalSession | null) {
  try {
    if (session?.state === "active")
      sessionStorage.setItem(storageKey, JSON.stringify(session));
    else sessionStorage.removeItem(storageKey);
  } catch {
    /* Session recovery is optional when storage is unavailable. */
  }
}

function restorePendingOpen(): PendingOpen | null {
  try {
    const raw = sessionStorage.getItem(pendingOpenStorageKey);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as PendingOpen;
    if (
      !/^[A-Za-z0-9._:-]{16,128}$/.test(parsed.idempotencyKey) ||
      parsed.columns < 20 ||
      parsed.columns > 500 ||
      parsed.rows < 5 ||
      parsed.rows > 300
    ) {
      sessionStorage.removeItem(pendingOpenStorageKey);
      return null;
    }
    return parsed;
  } catch {
    return null;
  }
}

function persistPendingOpen(pending: PendingOpen | null) {
  try {
    if (pending)
      sessionStorage.setItem(pendingOpenStorageKey, JSON.stringify(pending));
    else sessionStorage.removeItem(pendingOpenStorageKey);
  } catch {
    /* Retry continuity is optional when storage is unavailable. */
  }
}
