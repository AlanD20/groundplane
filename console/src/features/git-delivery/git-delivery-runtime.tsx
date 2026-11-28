import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ControllerRequestError } from "@/lib/controller-request-errors";
import { useGitDeliveryReplay } from "./replay-runtime";
import {
  approveDeployment,
  bindingView,
  bindingWrite,
  connectionView,
  deploymentView,
  readBinding,
  readConnections,
  readDeployments,
  readInheritedRunner,
  readRepositories,
  readRunnerSelectors,
  readWorkflows,
  saveBinding,
  startDeployment,
  type BindingState,
  type Connection,
  type Scope,
} from "./api-runtime";
import { GitDeliveryPanel } from "./git-delivery-panel";
import type {
  GitDeliveryBinding,
  GitDeployment,
  GitHubRepository,
  GitRunnerChoice,
} from "./types";

type Props = {
  serviceId: string;
  serviceName: string;
  environmentId: string;
  environmentName: string;
  projectId: string;
  tenantId: string;
  runnerRegistrationHref?: string;
  onConnect?: () => Promise<void> | void;
};

const settledPhases = new Set<GitDeployment["phase"]>([
  "succeeded",
  "failed",
  "superseded",
  "attention_required",
]);

function errorText(cause: unknown, fallback: string) {
  return cause instanceof Error ? cause.message : fallback;
}

function observation(current: GitDeployment, next: GitDeployment) {
  const before = current.updatedAt ?? current.createdAt;
  const after = next.updatedAt ?? next.createdAt;
  const utcKey = (time: string) =>
    time.replace(
      /(?:\.(\d+))?Z$/,
      (_, fraction: string | undefined) =>
        `.${(fraction ?? "").padEnd(9, "0")}Z`,
    );
  if (utcKey(after) < utcKey(before)) return current;
  return after === before &&
    next.phase === current.phase &&
    next.issue === current.issue &&
    next.githubRunUrl === current.githubRunUrl &&
    next.runner === current.runner &&
    next.githubRunnerId === current.githubRunnerId &&
    next.imageDigest === current.imageDigest &&
    next.fetchTaskId === current.fetchTaskId &&
    next.deployTaskId === current.deployTaskId &&
    next.releaseId === current.releaseId
    ? current
    : next;
}

function mergeDeployments(current: GitDeployment[], incoming: GitDeployment[]) {
  const merged = new Map(current.map((entry) => [entry.id, entry]));
  let changed = false;
  for (const entry of incoming) {
    const existing = merged.get(entry.id);
    const latest = existing ? observation(existing, entry) : entry;
    if (latest === existing) continue;
    merged.set(entry.id, latest);
    changed = true;
  }
  return changed
    ? [...merged.values()].sort((a, b) => b.id.localeCompare(a.id))
    : current;
}

export function GitDeliveryRuntime(props: Props) {
  return (
    <ServiceGitDelivery
      key={`${props.tenantId}/${props.projectId}/${props.environmentId}/${props.serviceId}`}
      {...props}
    />
  );
}

function ServiceGitDelivery({
  serviceId,
  serviceName,
  environmentId,
  environmentName,
  projectId,
  tenantId,
  runnerRegistrationHref,
  onConnect,
}: Props) {
  const [binding, setBinding] = useState<BindingState>();
  const [connections, setConnections] = useState<Connection[]>([]);
  const [repositories, setRepositories] = useState<GitHubRepository[]>([]);
  const [deployments, setDeployments] = useState<GitDeployment[]>([]);
  const [firstCursor, setFirstCursor] = useState<string>();
  const [olderCursor, setOlderCursor] = useState<string | null>();
  const [loadingOlder, setLoadingOlder] = useState(false);
  const [loading, setLoading] = useState(true);
  const [setupError, setSetupError] = useState("");
  const [historyError, setHistoryError] = useState("");
  const [setupObservation, setSetupObservation] = useState(0);
  const scopes = useMemo<Scope[]>(
    () => [
      { kind: "environment", id: environmentId },
      { kind: "project", id: projectId },
      { kind: "tenant", id: tenantId },
    ],
    [environmentId, projectId, tenantId],
  );
  const [readiness, setReadiness] = useState<{
    key: string;
    canDeploy?: boolean;
    error?: string;
    effectiveRunner?: GitRunnerChoice;
  }>();
  const currentRead = useRef<AbortController | null>(null);
  const olderRead = useRef<AbortController | null>(null);
  const refreshInProgress = useRef(false);
  const revisionAtEdit = useRef<number | undefined>(undefined);
  const { write, pending: isUnconfirmed } = useGitDeliveryReplay();

  const refreshHistory = useCallback(
    async (signal: AbortSignal) => {
      const page = await readDeployments(serviceId, signal);
      signal.throwIfAborted();
      const incoming = (page.items ?? []).map(deploymentView);
      setDeployments((current) => mergeDeployments(current, incoming));
      setFirstCursor(page.next_cursor);
      setHistoryError("");
    },
    [serviceId],
  );

  const refresh = useCallback(async () => {
    currentRead.current?.abort();
    const controller = new AbortController();
    currentRead.current = controller;
    refreshInProgress.current = true;
    const { signal } = controller;
    const setup = async () => {
      const [nextBinding, ...lists] = await Promise.all([
        readBinding(serviceId, signal),
        ...scopes.map((scope) =>
          readConnections(scope, signal).catch((error) => {
            if (error instanceof ControllerRequestError && error.status === 403)
              return [];
            throw error;
          }),
        ),
      ]);
      signal.throwIfAborted();
      setBinding((current) =>
        current && current.revision >= nextBinding.revision
          ? current
          : nextBinding,
      );
      setConnections(lists.flat());
      setSetupError("");
      setSetupObservation((current) => current + 1);
    };
    const [configuration, history] = await Promise.allSettled([
      setup(),
      refreshHistory(signal),
    ]);
    if (signal.aborted) return;
    refreshInProgress.current = false;
    if (configuration.status === "rejected") {
      setSetupError(
        errorText(
          configuration.reason,
          "Could not load GitHub deployment setup.",
        ),
      );
    }
    if (history.status === "rejected") {
      setHistoryError(
        errorText(
          history.reason,
          "Could not refresh GitHub deployment history.",
        ),
      );
    }
    setLoading(false);
  }, [refreshHistory, scopes, serviceId]);

  useEffect(() => {
    void refresh();
    return () => {
      currentRead.current?.abort();
      olderRead.current?.abort();
    };
  }, [refresh]);

  const active = deployments.some((entry) => !settledPhases.has(entry.phase));
  useEffect(() => {
    if (loading) return;
    const controller = new AbortController();
    const { signal } = controller;
    const delay = active ? 3_000 : 10_000;
    let timer: number;
    async function follow() {
      try {
        if (!document.hidden && !refreshInProgress.current) {
          await refreshHistory(signal);
        }
      } catch (cause) {
        if (!signal.aborted) {
          setHistoryError(
            errorText(cause, "Could not refresh GitHub deployment history."),
          );
        }
      } finally {
        if (!signal.aborted) timer = window.setTimeout(follow, delay);
      }
    }
    timer = window.setTimeout(follow, delay);
    return () => {
      controller.abort();
      window.clearTimeout(timer);
    };
  }, [active, loading, refreshHistory]);

  const saved = binding?.binding;
  const knownConnection = connections.find(
    (item) => item.id === saved?.connection_id,
  );
  const eligible = knownConnection && !knownConnection.revoked_at;
  const readinessKey = `${binding?.revision ?? 0}/${knownConnection?.generation ?? 0}`;
  const currentReadiness =
    readiness?.key === readinessKey ? readiness : undefined;
  const canDeploy = currentReadiness?.canDeploy;
  useEffect(() => {
    if (!saved || !eligible || !knownConnection) return;
    const controller = new AbortController();
    const { signal } = controller;
    const repositoryId = String(saved.repository_id);
    const choice = saved.inherit_runner_default
      ? readInheritedRunner(scopes, knownConnection, repositoryId, signal)
      : Promise.resolve({
          label: saved.runner_label ?? "",
          group: saved.runner_group,
          groupId: saved.runner_group_id
            ? String(saved.runner_group_id)
            : undefined,
        });
    void Promise.all([
      readRunnerSelectors(serviceId, saved.connection_id, repositoryId, signal),
      choice,
    ])
      .then(([selectors, effectiveRunner]) => {
        if (signal.aborted) return;
        if (!effectiveRunner) {
          setReadiness({
            key: readinessKey,
            canDeploy: false,
            error:
              "Set a repository Runner default in Settings or choose a Service override.",
          });
          return;
        }
        const match = selectors.find(
          (item) =>
            item.label.toLowerCase() === effectiveRunner.label.toLowerCase() &&
            (effectiveRunner.groupId
              ? item.groupId === effectiveRunner.groupId
              : (item.group ?? "") === (effectiveRunner.group ?? "")),
        );
        setReadiness({
          key: readinessKey,
          effectiveRunner,
          canDeploy: Boolean(
            match &&
            match.status !== "ineligible" &&
            match.status !== "offline",
          ),
        });
      })
      .catch((cause) => {
        if (!signal.aborted)
          setReadiness({
            key: readinessKey,
            error: errorText(cause, "Could not verify the saved Runner pool."),
          });
      });
    return () => controller.abort();
  }, [
    eligible,
    knownConnection,
    readinessKey,
    saved,
    scopes,
    serviceId,
    setupObservation,
  ]);

  useEffect(() => {
    if (!knownConnection || knownConnection.revoked_at) return;
    const controller = new AbortController();
    const { signal } = controller;
    void readRepositories(knownConnection, signal).then(
      (items) => {
        if (!signal.aborted) setRepositories(items);
      },
      () => {
        // The repository ID remains available from the saved binding.
      },
    );
    return () => controller.abort();
  }, [knownConnection]);

  const loadRepositories = useCallback(
    async (connectionId: string, signal: AbortSignal) => {
      const connection = connections.find((item) => item.id === connectionId);
      if (!connection || connection.revoked_at)
        throw new Error("This GitHub App connection is unavailable.");
      return readRepositories(connection, signal);
    },
    [connections],
  );

  const loadRunners = useCallback(
    (
      {
        connectionId,
        repositoryId,
      }: { connectionId: string; repositoryId: string },
      signal: AbortSignal,
    ) => readRunnerSelectors(serviceId, connectionId, repositoryId, signal),
    [serviceId],
  );

  async function loadWorkflows(
    {
      connectionId,
      repositoryId,
    }: { connectionId: string; repositoryId: string },
    signal: AbortSignal,
  ) {
    const connection = connections.find(
      (item) => item.id === connectionId && !item.revoked_at,
    );
    if (!connection)
      throw new Error("This GitHub App connection is unavailable.");
    return readWorkflows(connection, repositoryId, signal);
  }

  async function loadDefaultRunner(
    {
      connectionId,
      repositoryId,
    }: { connectionId: string; repositoryId: string },
    signal: AbortSignal,
  ) {
    const connection = connections.find(
      (item) => item.id === connectionId && !item.revoked_at,
    );
    if (!connection)
      throw new Error("This GitHub App connection is unavailable.");
    return readInheritedRunner(scopes, connection, repositoryId, signal);
  }

  async function commit(next: GitDeliveryBinding) {
    if (!binding)
      throw new Error("Reload GitHub deployment setup before saving.");
    const body = bindingWrite(next);
    revisionAtEdit.current ??= binding.revision;
    const revision = revisionAtEdit.current;
    try {
      const accepted = await write("binding", JSON.stringify(body), (key) =>
        saveBinding(serviceId, body, revision, key),
      );
      setBinding(accepted);
    } catch (cause) {
      if (
        cause instanceof ControllerRequestError &&
        [409, 412].includes(cause.status)
      ) {
        void refresh();
      }
      throw cause;
    } finally {
      if (!isUnconfirmed("binding")) revisionAtEdit.current = undefined;
    }
  }

  function remember(next: GitDeployment) {
    setDeployments((current) => mergeDeployments(current, [next]));
  }

  const historyCursor = olderCursor === undefined ? firstCursor : olderCursor;
  async function loadOlder() {
    if (!historyCursor || olderRead.current) return;
    const controller = new AbortController();
    olderRead.current = controller;
    setLoadingOlder(true);
    try {
      const page = await readDeployments(
        serviceId,
        controller.signal,
        historyCursor,
      );
      controller.signal.throwIfAborted();
      if (page.next_cursor === historyCursor) {
        throw new Error(
          "The Controller did not advance the deployment history cursor.",
        );
      }
      setDeployments((current) =>
        mergeDeployments(current, (page.items ?? []).map(deploymentView)),
      );
      setOlderCursor(page.next_cursor ?? null);
    } finally {
      if (!controller.signal.aborted) setLoadingOlder(false);
      olderRead.current = null;
    }
  }

  return (
    <GitDeliveryPanel
      serviceName={serviceName}
      environmentName={environmentName}
      binding={binding ? bindingView(binding) : null}
      connections={connections.map(connectionView)}
      repositories={repositories}
      deployments={deployments}
      loading={loading}
      error={setupError || historyError || undefined}
      canDeploy={canDeploy}
      effectiveRunner={currentReadiness?.effectiveRunner}
      deployUnavailableReason={
        currentReadiness?.error ||
        (canDeploy === false
          ? "No eligible registered Runner matches the saved label and group."
          : "Checking the saved Runner pool.")
      }
      runnerRegistrationHref={runnerRegistrationHref}
      onConnect={onConnect}
      onRefresh={refresh}
      onLoadRepositories={loadRepositories}
      onLoadRunnerSelectors={loadRunners}
      onLoadWorkflows={loadWorkflows}
      onLoadDefaultRunner={loadDefaultRunner}
      onBeginEdit={() => {
        if (!isUnconfirmed("binding"))
          revisionAtEdit.current = binding?.revision;
      }}
      onLoadOlder={historyCursor ? loadOlder : undefined}
      loadingOlder={loadingOlder}
      onSave={commit}
      onDeploy={async () => {
        const result = await write("deploy", "", (key) =>
          startDeployment(serviceId, key),
        );
        remember(deploymentView(result));
      }}
      onApprove={async (deploymentId) => {
        const result = await write(`approve:${deploymentId}`, "", (key) =>
          approveDeployment(serviceId, deploymentId, key),
        );
        remember(deploymentView(result));
      }}
    />
  );
}
