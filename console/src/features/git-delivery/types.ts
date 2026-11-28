export type GitRunnerChoice = {
  label: string;
  group?: string;
  groupId?: string;
  scopeKind?: "tenant" | "project" | "environment";
};

export type GitRunnerSelector = GitRunnerChoice & {
  status: "ready" | "busy" | "offline" | "unavailable" | "ineligible";
  detail?: string;
};

export type GitHubConnection = {
  id: string;
  name: string;
  status: "connected" | "unavailable";
};

export type GitHubWorkflow = {
  id: string;
  name: string;
  path: string;
};

export type GitHubRepository = {
  id: string;
  connectionId: string;
  name: string;
  defaultBranch?: string;
};

/** Repository and provider identifiers remain strings through the view layer. */
export type GitDeliveryBinding = {
  connectionId: string;
  repositoryId: string;
  branch: string;
  workflowId: string;
  workflowRef: string;
  workflowCommit?: string;
  dockerfile: string;
  buildContext: string;
  inheritRunnerDefault: boolean;
  runnerLabel: string;
  runnerGroup: string;
  runnerGroupId: string;
  automatic: boolean;
  requireApproval: boolean;
};

export type GitDeploymentPhase =
  | "queued"
  | "dispatching"
  | "building"
  | "awaiting_approval"
  | "fetch_pending"
  | "fetching"
  | "deploy_pending"
  | "deploying"
  | "succeeded"
  | "failed"
  | "superseded"
  | "attention_required";

export type GitDeployment = {
  id: string;
  phase: GitDeploymentPhase;
  commit: string;
  createdAt: string;
  updatedAt?: string;
  githubRunUrl?: string;
  runner?: string;
  githubRunnerId?: string;
  imageDigest?: string;
  fetchTaskId?: string;
  deployTaskId?: string;
  releaseId?: string;
  issue?: string;
};

export type GitDeliveryPanelProps = {
  serviceName: string;
  environmentName: string;
  binding: GitDeliveryBinding | null;
  connections: readonly GitHubConnection[];
  repositories?: readonly GitHubRepository[];
  runnerSelectors?: readonly GitRunnerSelector[];
  deployments: readonly GitDeployment[];
  loading?: boolean;
  error?: string;
  /** The server computes readiness for the saved binding. */
  canDeploy?: boolean;
  effectiveRunner?: GitRunnerChoice;
  deployUnavailableReason?: string;
  runnerRegistrationHref?: string;
  onLoadRepositories?: (
    connectionId: string,
    signal: AbortSignal,
  ) => Promise<readonly GitHubRepository[]>;
  onLoadRunnerSelectors?: (
    input: { connectionId: string; repositoryId: string },
    signal: AbortSignal,
  ) => Promise<readonly GitRunnerSelector[]>;
  onLoadWorkflows?: (
    input: { connectionId: string; repositoryId: string },
    signal: AbortSignal,
  ) => Promise<readonly GitHubWorkflow[]>;
  onLoadDefaultRunner?: (
    input: { connectionId: string; repositoryId: string },
    signal: AbortSignal,
  ) => Promise<GitRunnerChoice | undefined>;
  onConnect?: () => Promise<void> | void;
  onRefresh?: () => Promise<void> | void;
  onBeginEdit?: () => void;
  onLoadOlder?: () => Promise<void> | void;
  loadingOlder?: boolean;
  onSave: (binding: GitDeliveryBinding) => Promise<void>;
  onDeploy: () => Promise<void>;
  onApprove: (deploymentId: string) => Promise<void>;
};
