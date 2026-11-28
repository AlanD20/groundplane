import type { components, operations } from "@/lib/api.generated";
import { controllerRequest } from "@/lib/controller-json-request";
import type {
  GitDeliveryBinding,
  GitDeployment,
  GitHubConnection,
  GitHubRepository,
  GitHubWorkflow,
  GitRunnerChoice,
  GitRunnerSelector,
} from "./types";

type Schemas = components["schemas"];
export type Scope = Schemas["GitHubConnectionScope"];
export type BindingState = Schemas["GitDeploymentBindingState"];
export type BindingWrite = Schemas["GitDeploymentBindingWrite"];
export type Connection = Schemas["GitHubConnection"];
export type ConnectionWrite = Schemas["GitHubConnectionCreateRequest"];
export type ConnectionCredentials = Schemas["GitHubConnectionCredentialsWrite"];
export type RunnerDefaultState = Schemas["GitRepositoryRunnerDefaultState"];
export type RunnerDefaultWrite = Schemas["GitRepositoryRunnerDefaultWrite"];
type Deployment = Schemas["GitDeployment"];
type Repository = Schemas["GitHubRepository"];
type Workflow = Schemas["GitHubWorkflow"];
type Selector = Schemas["GitRunnerSelector"];
type BindingReadOperation = operations["git-deployment.binding-show"];

type Page<T> = { items?: T[]; next_cursor?: string };
const servicePath = (id: string) => `/services/${encodeURIComponent(id)}`;
const deploymentsPath = (id: string) => `${servicePath(id)}/git-deployments`;
const bindingPath = (id: string) => `${servicePath(id)}/git-deployment`;
const connectionPath = (id: string) =>
  `/github-connections/${encodeURIComponent(id)}`;
const scopeQuery = ({ kind, id }: Scope) =>
  new URLSearchParams({ scope_kind: kind, scope_id: id });

async function listPages<T>(
  path: string,
  query: URLSearchParams,
  signal: AbortSignal,
) {
  const records: T[] = [];
  const seen = new Set<string>();
  query.set("limit", "100");
  do {
    const page = await controllerRequest<Page<T>>(`${path}?${query}`, 200, {
      signal,
    });
    records.push(...(page.items ?? []));
    const cursor = page.next_cursor;
    if (!cursor) return records;
    if (seen.has(cursor))
      throw new Error("The Controller returned a repeated GitHub page cursor.");
    seen.add(cursor);
    query.set("cursor", cursor);
  } while (!signal.aborted);
  signal.throwIfAborted();
  return records;
}

export function readBinding(serviceId: string, signal: AbortSignal) {
  return controllerRequest<
    BindingReadOperation["responses"][200]["content"]["application/json"]
  >(bindingPath(serviceId), 200, { signal });
}

export function saveBinding(
  serviceId: string,
  body: BindingWrite,
  revision: number,
  idempotencyKey: string,
) {
  return controllerRequest<BindingState>(bindingPath(serviceId), 200, {
    method: "PUT",
    body,
    ifMatch: `"${revision}"`,
    idempotencyKey,
  });
}

export function readDeployments(
  serviceId: string,
  signal: AbortSignal,
  cursor?: string,
) {
  const query = new URLSearchParams({ limit: "50" });
  if (cursor) query.set("cursor", cursor);
  return controllerRequest<Page<Deployment>>(
    `${deploymentsPath(serviceId)}?${query}`,
    200,
    { signal },
  );
}

export function startDeployment(serviceId: string, idempotencyKey: string) {
  return controllerRequest<Deployment>(deploymentsPath(serviceId), 202, {
    method: "POST",
    body: {},
    idempotencyKey,
  });
}

export function approveDeployment(
  serviceId: string,
  deploymentId: string,
  idempotencyKey: string,
) {
  return controllerRequest<Deployment>(
    `${deploymentsPath(serviceId)}/${encodeURIComponent(deploymentId)}/approve`,
    202,
    {
      method: "POST",
      idempotencyKey,
    },
  );
}

export function readConnections(scope: Scope, signal: AbortSignal) {
  return listPages<Connection>(
    "/github-connections",
    scopeQuery(scope),
    signal,
  );
}

export function createConnection(
  body: ConnectionWrite,
  idempotencyKey: string,
) {
  return controllerRequest<Connection>("/github-connections", 201, {
    method: "POST",
    body,
    idempotencyKey,
  });
}

export async function readConnection(
  connection: Connection,
  signal: AbortSignal,
) {
  let revision = "";
  const item = await controllerRequest<Connection>(
    `${connectionPath(connection.id)}?${scopeQuery(connection.scope)}`,
    200,
    {
      signal,
      onResponseHeaders: (headers) => {
        revision = headers.get("ETag") ?? "";
      },
    },
  );
  if (!revision)
    throw new Error(
      "The Controller did not return the connection revision. Refresh before changing credentials.",
    );
  return { item, revision };
}

export function rotateConnection(
  connection: Connection,
  body: ConnectionCredentials,
  revision: string,
  idempotencyKey: string,
) {
  return controllerRequest<Connection>(
    `${connectionPath(connection.id)}/credentials?${scopeQuery(connection.scope)}`,
    200,
    {
      method: "PUT",
      body,
      ifMatch: revision,
      idempotencyKey,
    },
  );
}

export function revokeConnection(
  connection: Connection,
  revision: string,
  idempotencyKey: string,
) {
  return controllerRequest<void>(
    `${connectionPath(connection.id)}?${scopeQuery(connection.scope)}`,
    204,
    {
      method: "DELETE",
      ifMatch: revision,
      idempotencyKey,
    },
  );
}

export async function readRepositories(
  connection: Connection,
  signal: AbortSignal,
): Promise<GitHubRepository[]> {
  const items = await listPages<Repository>(
    `${connectionPath(connection.id)}/repositories`,
    scopeQuery(connection.scope),
    signal,
  );
  return items.map((item) => ({
    id: String(item.id),
    connectionId: connection.id,
    name: item.full_name,
    defaultBranch: item.default_branch,
  }));
}

function repositoryPath(connectionId: string, repositoryId: string) {
  return `${connectionPath(connectionId)}/repositories/${encodeURIComponent(repositoryId)}`;
}

function runnerDefaultPath(
  scope: Scope,
  connectionId: string,
  repositoryId: string,
) {
  return `${repositoryPath(connectionId, repositoryId)}/runner-default?${scopeQuery(scope)}`;
}

export function readRunnerDefault(
  scope: Scope,
  connectionId: string,
  repositoryId: string,
  signal: AbortSignal,
) {
  return controllerRequest<RunnerDefaultState>(
    runnerDefaultPath(scope, connectionId, repositoryId),
    200,
    { signal },
  );
}

export function saveRunnerDefault(
  scope: Scope,
  connectionId: string,
  repositoryId: string,
  body: RunnerDefaultWrite,
  revision: number,
  idempotencyKey: string,
) {
  return controllerRequest<RunnerDefaultState>(
    runnerDefaultPath(scope, connectionId, repositoryId),
    200,
    {
      method: "PUT",
      body,
      ifMatch: `"${revision}"`,
      idempotencyKey,
    },
  );
}

export function removeRunnerDefault(
  scope: Scope,
  connectionId: string,
  repositoryId: string,
  revision: number,
  idempotencyKey: string,
) {
  return controllerRequest<void>(
    runnerDefaultPath(scope, connectionId, repositoryId),
    204,
    {
      method: "DELETE",
      ifMatch: `"${revision}"`,
      idempotencyKey,
    },
  );
}

export async function readInheritedRunner(
  scopes: readonly Scope[],
  connection: Connection,
  repositoryId: string,
  signal: AbortSignal,
): Promise<GitRunnerChoice | undefined> {
  const end = scopes.findIndex(
    (scope) =>
      scope.id === connection.scope.id && scope.kind === connection.scope.kind,
  );
  if (end < 0)
    throw new Error("This GitHub App connection is outside the Service scope.");
  for (const scope of scopes.slice(0, end + 1)) {
    const { default: choice } = await readRunnerDefault(
      scope,
      connection.id,
      repositoryId,
      signal,
    );
    if (choice)
      return {
        label: choice.label,
        group: choice.group,
        groupId: choice.group_id ? String(choice.group_id) : undefined,
        scopeKind: scope.kind,
      };
  }
}

export async function readWorkflows(
  connection: Connection,
  repositoryId: string,
  signal: AbortSignal,
): Promise<GitHubWorkflow[]> {
  const items = await listPages<Workflow>(
    `${repositoryPath(connection.id, repositoryId)}/workflows`,
    scopeQuery(connection.scope),
    signal,
  );
  return items
    .filter((item) => item.state === "active")
    .map((item) => ({ id: String(item.id), name: item.name, path: item.path }));
}

export async function readRunnerSelectors(
  serviceId: string,
  connectionId: string,
  repositoryId: string,
  signal: AbortSignal,
): Promise<GitRunnerSelector[]> {
  const query = new URLSearchParams({
    connection_id: connectionId,
    repository_id: repositoryId,
  });
  const page = await controllerRequest<Page<Selector>>(
    `${servicePath(serviceId)}/git-runner-selectors?${query}`,
    200,
    { signal },
  );
  return (page.items ?? []).map((item) => ({
    label: item.label,
    group: item.group,
    groupId: item.group_id ? String(item.group_id) : undefined,
    status:
      item.available > 0
        ? "ready"
        : item.matching > 0
          ? "unavailable"
          : "ineligible",
    detail: `${item.available} available of ${item.matching} matching`,
  }));
}

export function connectionView(item: Connection): GitHubConnection {
  return {
    id: item.id,
    name: item.name,
    status: item.revoked_at ? "unavailable" : "connected",
  };
}

export function bindingView(state: BindingState): GitDeliveryBinding | null {
  const item = state.binding;
  if (!item) return null;
  return {
    connectionId: item.connection_id,
    repositoryId: String(item.repository_id),
    branch: item.branch,
    workflowId: String(item.workflow_id),
    workflowRef: item.workflow_ref,
    workflowCommit: item.workflow_commit ?? "",
    dockerfile: item.dockerfile,
    buildContext: item.build_context,
    inheritRunnerDefault: item.inherit_runner_default ?? false,
    runnerLabel: item.runner_label ?? "",
    runnerGroup: item.runner_group ?? "",
    runnerGroupId: item.runner_group_id ? String(item.runner_group_id) : "",
    automatic: item.automatic,
    requireApproval: item.require_approval,
  };
}

export function positiveInteger(value: string, label: string) {
  const id = Number(value);
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(id) || id <= 0) {
    throw new Error(`${label} must be a positive numeric GitHub ID.`);
  }
  return id;
}

export function bindingWrite(item: GitDeliveryBinding): BindingWrite {
  return {
    connection_id: item.connectionId,
    repository_id: positiveInteger(item.repositoryId, "Repository ID"),
    branch: item.branch,
    workflow_id: positiveInteger(item.workflowId, "Workflow ID"),
    workflow_ref: item.workflowRef,
    workflow_commit: item.workflowCommit || undefined,
    dockerfile: item.dockerfile,
    build_context: item.buildContext,
    ...(item.inheritRunnerDefault
      ? { inherit_runner_default: true }
      : {
          runner_label: item.runnerLabel,
          runner_group: item.runnerGroup || undefined,
          runner_group_id: item.runnerGroupId
            ? positiveInteger(item.runnerGroupId, "Runner group ID")
            : undefined,
        }),
    automatic: item.automatic,
    require_approval: item.requireApproval,
  };
}

export function deploymentView(item: Deployment): GitDeployment {
  const { runner_label, runner_group } = item.binding;
  return {
    id: item.id,
    phase: item.phase,
    commit: item.commit,
    createdAt: item.created_at,
    updatedAt: item.updated_at,
    githubRunUrl: item.github_url,
    runner: runner_label
      ? [runner_label, runner_group].filter(Boolean).join(" · ")
      : undefined,
    githubRunnerId: item.github_runner_id
      ? String(item.github_runner_id)
      : undefined,
    imageDigest: item.image_digest,
    fetchTaskId: item.fetch_task_id,
    deployTaskId: item.deploy_task_id,
    releaseId: item.release_id,
    issue: item.error_code,
  };
}
