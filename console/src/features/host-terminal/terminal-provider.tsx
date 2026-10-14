import { controllerRequest } from "@/lib/controller-json-request";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import {
  type CSSProperties,
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import {
  TerminalConnection,
  type HostTerminalSession,
} from "./terminal-connection";

export type { HostTerminalSession } from "./terminal-connection";

type TerminalTab = { id: string; name: string; connection: TerminalConnection };
type SavedTab = { name: string; session: HostTerminalSession };
type PendingOpen = { idempotencyKey: string; columns: number; rows: number };
const storageKey = "groundplane-host-terminal-tabs";
const pendingOpenStorageKey = "groundplane-host-terminal-new-tab";

type TerminalWorkspace = {
  tabs: TerminalTab[];
  activeId: string | null;
  visible: boolean;
  opening: boolean;
  maximized: boolean;
  height: number;
  openError: string | null;
  open: () => Promise<void>;
  newTab: () => Promise<void>;
  selectTab: (id: string) => void;
  renameTab: (id: string, name: string) => void;
  closeTab: (id: string) => void;
  show: () => void;
  hide: () => void;
  setMaximized: (value: boolean) => void;
  setHeight: (value: number) => void;
};

const TerminalContext = createContext<TerminalWorkspace | null>(null);
const noSubscribe = () => () => {};
const noSnapshot = () => null;

export function HostTerminalProvider({
  children,
}: {
  children: React.ReactNode;
}) {
  const [tabs, setTabs] = useState<TerminalTab[]>(restoreTabs);
  const [activeId, setActiveId] = useState<string | null>(tabs[0]?.id ?? null);
  const [visible, setVisible] = useState(false);
  const [opening, setOpening] = useState(false);
  const [maximized, setMaximized] = useState(false);
  const [height, setHeightState] = useState(360);
  const [openError, setOpenError] = useState<string | null>(null);
  const openingRef = useRef(false);
  const pendingOpenRef = useRef<PendingOpen | null>(restorePendingOpen());
  const tabsRef = useRef(tabs);
  tabsRef.current = tabs;

  useEffect(() => {
    const save = () => persistTabs(tabs);
    save();
    const unsubscribe = tabs.map((tab) => tab.connection.subscribe(save));
    return () => {
      for (const release of unsubscribe) release();
    };
  }, [tabs]);

  useEffect(
    () => () => {
      for (const tab of tabsRef.current) tab.connection.dispose();
    },
    [],
  );

  const newTab = useCallback(async () => {
    setVisible(true);
    if (openingRef.current) return;
    openingRef.current = true;
    setOpening(true);
    setOpenError(null);
    const pending = pendingOpenRef.current ?? {
      idempotencyKey: newULID(),
      columns: 100,
      rows: 28,
    };
    pendingOpenRef.current = pending;
    saveOptional(pendingOpenStorageKey, pending);
    try {
      const session = await controllerRequest<HostTerminalSession>(
        "/host/terminal",
        201,
        {
          method: "POST",
          body: { columns: pending.columns, rows: pending.rows },
          idempotencyKey: pending.idempotencyKey,
        },
      );
      pendingOpenRef.current = null;
      saveOptional(pendingOpenStorageKey, null);
      setTabs((current) =>
        current.some((tab) => tab.id === session.id)
          ? current
          : [
              ...current,
              {
                id: session.id,
                name: `Terminal ${current.length + 1}`,
                connection: new TerminalConnection(session),
              },
            ],
      );
      setActiveId(session.id);
    } catch (cause) {
      // A refused request cannot have opened a shell. An uncertain outcome must
      // keep its key so the next click recovers that shell instead of duplicating it.
      if (
        cause instanceof ControllerRequestError &&
        cause.status >= 400 &&
        cause.status < 500
      ) {
        pendingOpenRef.current = null;
        saveOptional(pendingOpenStorageKey, null);
      }
      setOpenError(
        cause instanceof Error
          ? cause.message
          : "Unable to open host terminal.",
      );
    } finally {
      openingRef.current = false;
      setOpening(false);
    }
  }, []);

  const open = useCallback(async () => {
    setVisible(true);
    if (tabsRef.current.length === 0) await newTab();
    else {
      const selected =
        tabsRef.current.find((tab) => tab.id === activeId) ??
        tabsRef.current[0];
      setActiveId(selected.id);
      selected.connection.connect();
    }
  }, [newTab, activeId]);

  const selectTab = useCallback((id: string) => {
    setActiveId(id);
    setVisible(true);
  }, []);
  const renameTab = useCallback((id: string, name: string) => {
    const next = name.trim().slice(0, 40);
    if (next)
      setTabs((current) =>
        current.map((tab) => (tab.id === id ? { ...tab, name: next } : tab)),
      );
  }, []);
  const closeTab = useCallback((id: string) => {
    const tab = tabsRef.current.find((item) => item.id === id);
    if (!tab || tab.connection.getSnapshot().session.state === "active") return;
    tab.connection.dispose();
    const remaining = tabsRef.current.filter((item) => item.id !== id);
    setTabs(remaining);
    setActiveId((current) =>
      current === id ? (remaining[0]?.id ?? null) : current,
    );
    if (remaining.length === 0) {
      setVisible(false);
      setMaximized(false);
    }
  }, []);
  const setHeight = useCallback((value: number) => {
    setHeightState(Math.max(220, Math.min(value, window.innerHeight * 0.75)));
  }, []);
  const show = useCallback(() => setVisible(true), []);
  const hide = useCallback(() => {
    setVisible(false);
    setMaximized(false);
  }, []);

  const value = useMemo<TerminalWorkspace>(
    () => ({
      tabs,
      activeId,
      visible,
      opening,
      maximized,
      height,
      openError,
      open,
      newTab,
      selectTab,
      renameTab,
      closeTab,
      show,
      hide,
      setMaximized,
      setHeight,
    }),
    [
      tabs,
      activeId,
      visible,
      opening,
      maximized,
      height,
      openError,
      open,
      newTab,
      selectTab,
      renameTab,
      closeTab,
      show,
      hide,
      setHeight,
    ],
  );

  return (
    <TerminalContext.Provider value={value}>
      <div
        className="contents"
        style={
          {
            "--terminal-dock-height":
              visible && !maximized && (tabs.length || opening || openError)
                ? `${height}px`
                : "0px",
          } as CSSProperties
        }
      >
        {children}
      </div>
    </TerminalContext.Provider>
  );
}

export function useHostTerminal(id?: string) {
  const workspace = useContext(TerminalContext);
  if (!workspace)
    throw new Error("useHostTerminal must be used within HostTerminalProvider");
  const connection = workspace.tabs.find(
    (tab) => tab.id === (id ?? workspace.activeId),
  )?.connection;
  const state = useSyncExternalStore(
    connection?.subscribe ?? noSubscribe,
    connection?.getSnapshot ?? noSnapshot,
  );
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (state?.session.state !== "active") return;
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, [state?.session.state]);
  return {
    ...workspace,
    connection,
    session: state?.session ?? null,
    connected: state?.connected ?? false,
    error: state?.error ?? null,
    remainingSeconds:
      state?.session.state === "active"
        ? Math.max(
            0,
            Math.ceil((Date.parse(state.session.expires_at) - now) / 1_000),
          )
        : 0,
    terminate: connection?.terminate,
  };
}

function restoreTabs(): TerminalTab[] {
  try {
    const raw = sessionStorage.getItem(storageKey);
    if (!raw) return [];
    const parsed = JSON.parse(raw) as SavedTab[];
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(
        (tab) =>
          tab != null &&
          typeof tab.name === "string" &&
          tab.session?.id &&
          tab.session.state === "active" &&
          Date.parse(tab.session.expires_at) > Date.now(),
      )
      .slice(0, 4)
      .map((tab) => ({
        id: tab.session.id,
        name: tab.name,
        connection: new TerminalConnection(tab.session),
      }));
  } catch {
    return [];
  }
}

function persistTabs(tabs: TerminalTab[]) {
  saveOptional(
    storageKey,
    tabs
      .filter((tab) => tab.connection.getSnapshot().session.state === "active")
      .map((tab) => ({
        name: tab.name,
        session: tab.connection.getSnapshot().session,
      })),
  );
}

function restorePendingOpen(): PendingOpen | null {
  try {
    const raw = sessionStorage.getItem(pendingOpenStorageKey);
    if (!raw) return null;
    const pending = JSON.parse(raw) as PendingOpen;
    return /^[A-Za-z0-9._:-]{16,128}$/.test(pending.idempotencyKey) &&
      pending.columns >= 20 &&
      pending.columns <= 500 &&
      pending.rows >= 5 &&
      pending.rows <= 300
      ? pending
      : null;
  } catch {
    return null;
  }
}

function saveOptional(key: string, value: SavedTab[] | PendingOpen | null) {
  try {
    if (value === null) sessionStorage.removeItem(key);
    else sessionStorage.setItem(key, JSON.stringify(value));
  } catch {
    /* Session recovery is optional when browser storage is unavailable. */
  }
}
