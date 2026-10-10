import { useCallback, useEffect, useRef, useState } from "react";
import { controllerRequest } from "@/lib/controller-json-request";
import { controllerUpdateRejected } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import type { SoftwareActivation, SoftwarePreparation, SoftwarePreparationPage, SoftwareRequest } from "./software-api";

const pendingKey = "groundplane.software.preparation.pending";
type Pending = { key: string; intent: { kind: "prepare"; request: SoftwareRequest } | { kind: "apply"; task: string } };
const activationKey = "groundplane.software.activation.task";

function readPending(): Pending | null {
  const raw = sessionStorage.getItem(pendingKey);
  if (!raw) return null;
  const value: unknown = JSON.parse(raw);
  if (!value || typeof value !== "object" || !("key" in value) || typeof value.key !== "string" ||
    !("intent" in value) || !value.intent || typeof value.intent !== "object" || !("kind" in value.intent)) {
    throw new Error("The retained software request is invalid; it has not been submitted again.");
  }
  const intent = value.intent;
  if (intent.kind === "apply" && "task" in intent && typeof intent.task === "string" && /^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(intent.task))
    return { key: value.key, intent: { kind: "apply", task: intent.task } };
  if (intent.kind === "prepare" && "request" in intent && intent.request && typeof intent.request === "object") {
    const request = intent.request;
    if ("ref" in request && typeof request.ref === "string" && "selection" in request &&
      ["controller", "agent", "both"].includes(String(request.selection)) && "source_kind" in request &&
      ["release", "source_ref"].includes(String(request.source_kind))) {
      return { key: value.key, intent: { kind: "prepare", request: { ref: request.ref,
        selection: request.selection as SoftwareRequest["selection"], source_kind: request.source_kind as SoftwareRequest["source_kind"] } } };
    }
  }
  throw new Error("The retained software request is invalid; it has not been submitted again.");
}

export function useSoftwarePreparations(active: boolean) {
  const [items, setItems] = useState<SoftwarePreparation[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pending, setPending] = useState<Pending | null>(null);
  const [publishing, setPublishing] = useState(false);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [activation, setActivation] = useState<SoftwareActivation | null>(null);
  const olderLoaded = useRef(false);
  const generation = useRef(0);
  const mutating = useRef(false);

  useEffect(() => {
    try { setPending(readPending()); }
    catch (cause) { setError(message(cause)); }
  }, []);

  const refresh = useCallback(async (signal?: AbortSignal) => {
    const current = ++generation.current;
    setLoading(true);
    try {
      const page = await controllerRequest<SoftwarePreparationPage>("/software/preparations?limit=100", 200, { signal });
      if (signal?.aborted || current !== generation.current) return;
      setItems((existing) => olderLoaded.current ? merge(page.items ?? [], existing) : page.items ?? []);
      if (!olderLoaded.current) setNextCursor(page.next_cursor ?? null);
      const task = sessionStorage.getItem(activationKey);
      if (task && /^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(task)) {
        const result = await controllerRequest<SoftwareActivation>(`/software/activations/${task}`, 200, { signal });
        if (!signal?.aborted && current === generation.current) setActivation(result);
      }
      setLoaded(true);
      setError(null);
    } catch (cause) {
      if (!signal?.aborted && current === generation.current) setError(message(cause));
    } finally {
      if (!signal?.aborted && current === generation.current) setLoading(false);
    }
  }, []);

  // Poll only while this workspace is visible. Revalidation keeps the form and
  // loaded rows mounted; a disconnected Controller never dispatches new work.
  useEffect(() => {
    if (!active) return;
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    const poll = async () => {
      await refresh(abort.signal);
      if (!abort.signal.aborted) timer = setTimeout(() => void poll(), 5000);
    };
    void poll();
    return () => { abort.abort(); if (timer) clearTimeout(timer); };
  }, [active, refresh]);

  const publish = useCallback(async (retained: Pending) => {
    if (mutating.current) return;
    mutating.current = true;
    setPublishing(true);
    setError(null);
    try {
      const intent = retained.intent;
      const accepted = await controllerRequest<{ task_id: string }>(intent.kind === "prepare" ? "/software/preparations" : `/software/preparations/${intent.task}/apply`, 202,
        { method: "POST", ...(intent.kind === "prepare" ? { body: intent.request } : {}), idempotencyKey: retained.key });
      if (intent.kind === "apply") sessionStorage.setItem(activationKey, accepted.task_id);
      sessionStorage.removeItem(pendingKey);
      setPending(null);
      await refresh();
    } catch (cause) {
      if (controllerUpdateRejected(cause)) {
        sessionStorage.removeItem(pendingKey);
        setPending(null);
      }
      setError(message(cause));
    } finally {
      mutating.current = false;
      setPublishing(false);
    }
  }, [refresh]);

  const submit = useCallback(async (intent: Pending["intent"]) => {
    if (mutating.current || pending) return;
    const retained = { key: newULID(), intent };
    try {
      sessionStorage.setItem(pendingKey, JSON.stringify(retained));
      setPending(retained);
      await publish(retained);
    } catch (cause) { setError(message(cause)); }
  }, [pending, publish]);

  const loadOlder = async () => {
    if (!nextCursor || loading) return;
    setLoading(true);
    try {
      const page = await controllerRequest<SoftwarePreparationPage>(`/software/preparations?limit=100&cursor=${encodeURIComponent(nextCursor)}`, 200);
      olderLoaded.current = true;
      setItems((existing) => merge(existing, page.items ?? []));
      setNextCursor(page.next_cursor ?? null);
    } catch (cause) { setError(message(cause)); }
    finally { setLoading(false); }
  };
  return { items, loaded, loading, error, publishing, pending, nextCursor, refresh, loadOlder, activation,
    prepare: (request: SoftwareRequest) => submit({ kind: "prepare", request }),
    apply: (task: string) => submit({ kind: "apply", task }),
    resolve: () => pending ? publish(pending) : Promise.resolve() };
}

function merge(first: SoftwarePreparation[], second: SoftwarePreparation[]) {
  const seen = new Set(first.map((item) => item.task_id));
  return [...first, ...second.filter((item) => !seen.has(item.task_id))];
}

function message(cause: unknown) {
  return cause instanceof Error ? cause.message : "Unable to load or prepare software";
}
