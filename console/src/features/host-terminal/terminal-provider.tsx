import type { components } from "@/lib/api.generated";
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
  type TerminalSession,
} from "./terminal-connection";

export type { TerminalSession } from "./terminal-connection";

type TerminalTab = { id: string; name: string; connection: TerminalConnection };
type SavedTab = { name: string; session: TerminalSession };
type ContainerOptions = components["schemas"]["ContainerTerminalOpen"];
type OpenTarget = {
  path: string;
  body: ContainerOptions | { columns: number; rows: number };
  name: string;
};
type PendingOpen = OpenTarget & { idempotencyKey: string };
const storageKey = "groundplane-terminal-tabs";
const pendingOpenStorageKey = "groundplane-terminal-new-tab";

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
  openContainer: (
    serviceID: string,
    name: string,
    options: ContainerOptions,
  ) => Promise<void>;
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

  const openTab = useCallback(async (target: OpenTarget) => {
    setVisible(true);
    if (openingRef.current) throw new Error("A terminal is already opening");
    openingRef.current = true;
    setOpening(true);
    setOpenError(null);
    const previous = pendingOpenRef.current;
    if (
      previous &&
      (previous.path !== target.path ||
        JSON.stringify(previous.body) !== JSON.stringify(target.body))
    ) {
      openingRef.current = false;
      setOpening(false);
      throw new Error(
        "Retry the previous terminal request before opening a different shell; its outcome is still unknown.",
      );
    }
    const pending = previous ?? { ...target, idempotencyKey: newULID() };
    pendingOpenRef.current = pending;
    saveOptional(pendingOpenStorageKey, pending);
    try {
      const session = await controllerRequest<TerminalSession>(
        pending.path,
        201,
        {
          method: "POST",
          body: pending.body,
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
                name: pending.name,
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
        cause instanceof Error ? cause.message : "Unable to open terminal.",
      );
      throw cause;
    } finally {
      openingRef.current = false;
      setOpening(false);
    }
  }, []);

  const newTab = useCallback(async () => {
    try {
      await openTab({
        path: "/host/terminal",
        body: { columns: 100, rows: 28 },
        name: "Host shell",
      });
    } catch (cause) {
      setOpenError(
        cause instanceof Error ? cause.message : "Unable to open terminal.",
      );
    }
  }, [openTab]);
  const openContainer = useCallback(
    async (serviceID: string, name: string, options: ContainerOptions) => {
      await openTab({
        path: "/services/" + serviceID + "/terminal",
        body: options,
        name: name.slice(0, 40),
      });
    },
    [openTab],
  );

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
    tabsRef.current = remaining;
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
      openContainer,
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
      openContainer,
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
      (pending.path === "/host/terminal" ||
        /^\/services\/svc_[0-9A-HJKMNP-TV-Z]{26}\/terminal$/.test(
          pending.path,
        )) &&
      typeof pending.name === "string" &&
      pending.body &&
      pending.body.columns >= 20 &&
      pending.body.columns <= 500 &&
      pending.body.rows >= 5 &&
      pending.body.rows <= 300
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
