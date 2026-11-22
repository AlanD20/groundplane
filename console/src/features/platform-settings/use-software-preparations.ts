import { useCallback, useEffect, useRef, useState } from "react";
import { controllerRequest } from "@/lib/controller-json-request";
import { controllerUpdateRejected } from "@/lib/controller-request-errors";
import { newULID } from "@/lib/utils";
import { requestTask } from "@/features/task/api";
import type {
  SoftwareActivation,
  SoftwarePreparation,
  SoftwarePreparationPage,
  SoftwareRequest,
} from "./software-api";

const pendingKey = "groundplane.software.preparation.pending";
type Pending = {
  key: string;
  intent:
    | { kind: "prepare"; request: SoftwareRequest }
    | { kind: "apply"; task: string; after: string[] };
};
const activationKey = "groundplane.software.activation.task";
const preparationKey = "groundplane.software.preparation.task";

function readAfter(value: unknown): string[] {
  if (
    !Array.isArray(value) ||
    value.length > 16 ||
    value.some(
      (id) =>
        typeof id !== "string" || !/^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(id),
    ) ||
    new Set(value).size !== value.length
  )
    throw new Error(
      "The retained software prerequisites are invalid; the request has not been submitted again.",
    );
  return value;
}

function readPending(): Pending | null {
  const raw = sessionStorage.getItem(pendingKey);
  if (!raw) return null;
  const value: unknown = JSON.parse(raw);
  if (
    !value ||
    typeof value !== "object" ||
    !("key" in value) ||
    typeof value.key !== "string" ||
    !("intent" in value) ||
    !value.intent ||
    typeof value.intent !== "object" ||
    !("kind" in value.intent)
  ) {
    throw new Error(
      "The retained software request is invalid; it has not been submitted again.",
    );
  }
  const intent = value.intent;
  if (
    intent.kind === "apply" &&
    "task" in intent &&
    typeof intent.task === "string" &&
    /^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(intent.task) &&
    "after" in intent
  )
    return {
      key: value.key,
      intent: {
        kind: "apply",
        task: intent.task,
        after: readAfter(intent.after),
      },
    };
  if (
    intent.kind === "prepare" &&
    "request" in intent &&
    intent.request &&
    typeof intent.request === "object"
  ) {
    const request = intent.request;
    if (
      "ref" in request &&
      typeof request.ref === "string" &&
      "selection" in request &&
      ["controller", "agent", "console", "both", "all"].includes(
        String(request.selection),
      ) &&
      "source_kind" in request &&
      ["release", "source_ref"].includes(String(request.source_kind)) &&
      "after" in request
    ) {
      return {
        key: value.key,
        intent: {
          kind: "prepare",
          request: {
            ref: request.ref,
            selection: request.selection as SoftwareRequest["selection"],
            source_kind: request.source_kind as SoftwareRequest["source_kind"],
            after: readAfter(request.after),
          },
        },
      };
    }
  }
  throw new Error(
    "The retained software request is invalid; it has not been submitted again.",
  );
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
  const [acceptedTaskId, setAcceptedTaskId] = useState<string | null>(null);
  const [acceptedStatus, setAcceptedStatus] = useState<string | null>(null);
  const [acceptedKind, setAcceptedKind] = useState<"prepare" | "apply" | null>(
    null,
  );
  const olderLoaded = useRef(false);
  const generation = useRef(0);
  const mutating = useRef(false);

  useEffect(() => {
    try {
      setPending(readPending());
    } catch (cause) {
      setError(message(cause));
    }
  }, []);

  const refresh = useCallback(async (signal?: AbortSignal) => {
    const current = ++generation.current;
    setLoading(true);
    try {
      const page = await controllerRequest<SoftwarePreparationPage>(
        "/software/preparations?limit=100",
        200,
        { signal },
      );
      if (signal?.aborted || current !== generation.current) return;
      setItems((existing) =>
        olderLoaded.current
          ? merge(page.items ?? [], existing)
          : (page.items ?? []),
      );
      if (!olderLoaded.current) setNextCursor(page.next_cursor ?? null);
      const task = sessionStorage.getItem(activationKey);
      if (task && /^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(task)) {
        const detail = await requestTask(task, signal);
        if (signal?.aborted || current !== generation.current) return;
        setAcceptedTaskId(task);
        setAcceptedStatus(detail.task.status);
        setAcceptedKind("apply");
        if (detail.work.items.length > 0) {
          const result = await controllerRequest<SoftwareActivation>(
            `/software/activations/${task}`,
            200,
            { signal },
          );
          if (!signal?.aborted && current === generation.current)
            setActivation(result);
        } else setActivation(null);
      } else {
        const preparation = sessionStorage.getItem(preparationKey);
        if (preparation && /^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(preparation)) {
          const detail = await requestTask(preparation, signal);
          if (signal?.aborted || current !== generation.current) return;
          setAcceptedTaskId(preparation);
          setAcceptedStatus(detail.task.status);
          setAcceptedKind("prepare");
        }
      }
      setLoaded(true);
      setError(null);
    } catch (cause) {
      if (!signal?.aborted && current === generation.current)
        setError(message(cause));
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
    return () => {
      abort.abort();
      if (timer) clearTimeout(timer);
    };
  }, [active, refresh]);

  const publish = useCallback(
    async (retained: Pending) => {
      if (mutating.current)
        throw new Error("A software request is already being submitted");
      mutating.current = true;
      setPublishing(true);
      setError(null);
      try {
        const intent = retained.intent;
        const accepted = await controllerRequest<{ task_id: string }>(
          intent.kind === "prepare"
            ? "/software/preparations"
            : `/software/preparations/${intent.task}/apply`,
          202,
          {
            method: "POST",
            body:
              intent.kind === "prepare"
                ? intent.request
                : { after: intent.after },
            idempotencyKey: retained.key,
          },
        );
        if (intent.kind === "apply")
          sessionStorage.setItem(activationKey, accepted.task_id);
        else {
          sessionStorage.setItem(preparationKey, accepted.task_id);
          sessionStorage.removeItem(activationKey);
        }
        setAcceptedTaskId(accepted.task_id);
        setAcceptedKind(intent.kind);
        setAcceptedStatus(null);
        sessionStorage.removeItem(pendingKey);
        setPending(null);
        await refresh();
        return accepted.task_id;
      } catch (cause) {
        if (controllerUpdateRejected(cause)) {
          sessionStorage.removeItem(pendingKey);
          setPending(null);
        }
        setError(message(cause));
        throw cause;
      } finally {
        mutating.current = false;
        setPublishing(false);
      }
    },
    [refresh],
  );

  const submit = useCallback(
    async (intent: Pending["intent"]) => {
      if (mutating.current || pending)
        throw new Error(
          "Resolve the retained software request before submitting another",
        );
      const retained = { key: newULID(), intent };
      try {
        sessionStorage.setItem(pendingKey, JSON.stringify(retained));
        setPending(retained);
        return await publish(retained);
      } catch (cause) {
        setError(message(cause));
        throw cause;
      }
    },
    [pending, publish],
  );

  const loadOlder = async () => {
    if (!nextCursor || loading) return;
    setLoading(true);
    try {
      const page = await controllerRequest<SoftwarePreparationPage>(
        `/software/preparations?limit=100&cursor=${encodeURIComponent(nextCursor)}`,
        200,
      );
      olderLoaded.current = true;
      setItems((existing) => merge(existing, page.items ?? []));
      setNextCursor(page.next_cursor ?? null);
    } catch (cause) {
      setError(message(cause));
    } finally {
      setLoading(false);
    }
  };
  return {
    items,
    loaded,
    loading,
    error,
    publishing,
    pending,
    nextCursor,
    refresh,
    loadOlder,
    activation,
    acceptedTaskId,
    acceptedStatus,
    acceptedKind,
    prepare: (request: SoftwareRequest) =>
      submit({ kind: "prepare", request }).catch(() => undefined),
    apply: (task: string, after: string[] = []) =>
      submit({ kind: "apply", task, after }),
    resolve: () =>
      pending ? publish(pending).catch(() => undefined) : Promise.resolve(),
  };
}

function merge(first: SoftwarePreparation[], second: SoftwarePreparation[]) {
  const seen = new Set(first.map((item) => item.task_id));
  return [...first, ...second.filter((item) => !seen.has(item.task_id))];
}

function message(cause: unknown) {
  return cause instanceof Error
    ? cause.message
    : "Unable to load or prepare software";
}
