import { useEffect, useEffectEvent, useId, useState } from "react";
import { Link } from "react-router-dom";
import { FormActions } from "@/components/common/form-actions";
import { StatusBadge } from "@/components/common/status-badge";
import { editorFooterClassName } from "@/components/common/workspace-section";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { FormSection } from "@/components/ui/form-section";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { ChoiceError, Field } from "./binding-fields";
import { WorkflowFields } from "./workflow-fields";
import type {
  GitDeliveryBinding,
  GitDeliveryPanelProps,
  GitRunnerChoice,
} from "./types";

type ChoiceResult<T> = {
  key: string;
  values?: readonly T[];
  error?: string;
};

function useChoices<T>(
  key: string,
  load?: (signal: AbortSignal) => Promise<readonly T[]>,
) {
  const [result, setResult] = useState<ChoiceResult<T>>();
  const [attempt, setAttempt] = useState(0);
  const request = useEffectEvent((signal: AbortSignal) => load?.(signal));
  const enabled = Boolean(key && load);

  useEffect(() => {
    if (!enabled) return;
    const controller = new AbortController();
    const loadChoices = async () => request(controller.signal);
    void loadChoices().then(
      (values) => {
        if (!controller.signal.aborted) setResult({ key, values });
      },
      () => {
        if (!controller.signal.aborted)
          setResult({ key, error: "Could not load options. Try again." });
      },
    );
    return () => controller.abort();
  }, [key, enabled, attempt]);

  const current = enabled && result?.key === key ? result : undefined;
  return {
    values: current?.values,
    error: current?.error,
    loading: enabled && !current,
    retry: () => {
      setResult(undefined);
      setAttempt((value) => value + 1);
    },
  };
}

function runnerKey(choice: GitRunnerChoice) {
  return JSON.stringify([
    choice.label.toLowerCase(),
    choice.groupId || choice.group,
  ]);
}

function newBinding(connectionId: string): GitDeliveryBinding {
  return {
    connectionId,
    repositoryId: "",
    branch: "",
    workflowId: "",
    workflowRef: "",
    dockerfile: "Dockerfile",
    buildContext: ".",
    inheritRunnerDefault: false,
    runnerLabel: "",
    runnerGroup: "",
    runnerGroupId: "",
    automatic: false,
    requireApproval: true,
  };
}

export function BindingEditor({
  binding,
  connections,
  repositories,
  runnerSelectors,
  runnerRegistrationHref,
  onLoadRepositories,
  onLoadRunnerSelectors,
  onLoadWorkflows,
  onLoadDefaultRunner,
  onSave,
  onCancel,
}: Pick<
  GitDeliveryPanelProps,
  | "binding"
  | "connections"
  | "repositories"
  | "runnerSelectors"
  | "runnerRegistrationHref"
  | "onLoadRepositories"
  | "onLoadRunnerSelectors"
  | "onLoadWorkflows"
  | "onLoadDefaultRunner"
  | "onSave"
> & { onCancel: () => void }) {
  const id = useId();
  const available = connections.filter(
    (connection) => connection.status === "connected",
  );
  const [initial] = useState(
    () => binding ?? newBinding(available.length === 1 ? available[0].id : ""),
  );
  const [draft, setDraft] = useState(initial);
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState("");
  const repositoryChoices = useChoices(
    draft.connectionId,
    onLoadRepositories
      ? (signal) => onLoadRepositories(draft.connectionId, signal)
      : undefined,
  );
  const selection = {
    connectionId: draft.connectionId,
    repositoryId: draft.repositoryId,
  };
  const selectionKey = draft.repositoryId ? JSON.stringify(selection) : "";
  const workflows = useChoices(
    selectionKey,
    onLoadWorkflows
      ? (signal) => onLoadWorkflows(selection, signal)
      : undefined,
  );
  const defaults = useChoices(
    selectionKey,
    onLoadDefaultRunner
      ? async (signal) => {
          const runner = await onLoadDefaultRunner(selection, signal);
          return runner ? [runner] : [];
        }
      : undefined,
  );
  const defaultRunner = defaults.values?.[0];
  const runnerChoices = useChoices(
    selectionKey,
    onLoadRunnerSelectors
      ? (signal) => onLoadRunnerSelectors(selection, signal)
      : undefined,
  );
  const visibleRepos = (repositoryChoices.values ?? repositories ?? []).filter(
    (item) => item.connectionId === draft.connectionId,
  );
  const repo = visibleRepos.find((item) => item.id === draft.repositoryId);
  const runners = runnerChoices.values ?? runnerSelectors ?? [];
  const explicitRunner = {
    label: draft.runnerLabel,
    group: draft.runnerGroup,
    groupId: draft.runnerGroupId,
  };
  const effectiveRunner = draft.inheritRunnerDefault
    ? defaultRunner
    : explicitRunner;
  const resolvedRunner = runners.find(
    (runner) =>
      effectiveRunner &&
      runner.label.toLowerCase() === effectiveRunner.label.toLowerCase() &&
      (effectiveRunner.groupId
        ? runner.groupId === effectiveRunner.groupId
        : (runner.group ?? "") === (effectiveRunner.group ?? "")),
  );
  const selectedKey = draft.inheritRunnerDefault
    ? "default"
    : draft.runnerLabel
      ? runnerKey(resolvedRunner ?? explicitRunner)
      : null;
  const runnerIsEligible =
    resolvedRunner?.status === "ready" ||
    resolvedRunner?.status === "busy" ||
    resolvedRunner?.status === "offline" ||
    resolvedRunner?.status === "unavailable";
  const automaticRefConflict =
    draft.automatic &&
    draft.branch.trim() !== "" &&
    draft.branch.trim().replace(/^refs\/heads\//, "") ===
      draft.workflowRef.trim().replace(/^refs\/heads\//, "");
  const complete =
    available.some((connection) => connection.id === draft.connectionId) &&
    Boolean(repo && runnerIsEligible) &&
    !repositoryChoices.loading &&
    !runnerChoices.loading &&
    !repositoryChoices.error &&
    !runnerChoices.error &&
    (!draft.inheritRunnerDefault || (!defaults.loading && !defaults.error)) &&
    !workflows.loading &&
    !automaticRefConflict &&
    /^[1-9]\d*$/.test(draft.workflowId.trim()) &&
    (!draft.workflowCommit?.trim() ||
      /^[a-f0-9]{40}$/i.test(draft.workflowCommit.trim())) &&
    [
      draft.branch,
      draft.workflowRef,
      draft.dockerfile,
      draft.buildContext,
    ].every((value) => value.trim() !== "");
  const dirty = JSON.stringify(initial) !== JSON.stringify(draft);

  function change(changes: Partial<GitDeliveryBinding>) {
    setDraft((current) => ({ ...current, ...changes }));
    setSaveError("");
  }

  async function save() {
    if (!complete || saving) return;
    setSaving(true);
    setSaveError("");
    try {
      await onSave({
        ...draft,
        workflowId: draft.workflowId.trim(),
        workflowRef: draft.workflowRef.trim(),
        workflowCommit: draft.workflowCommit?.trim().toLowerCase() || undefined,
        branch: draft.branch.trim(),
        dockerfile: draft.dockerfile.trim(),
        buildContext: draft.buildContext.trim(),
      });
    } catch (error) {
      setSaveError(
        error instanceof Error
          ? error.message
          : "Could not save GitHub deployment.",
      );
    } finally {
      setSaving(false);
    }
  }

  return (
    <form
      className="space-y-4"
      onSubmit={(event) => {
        event.preventDefault();
        void save();
      }}
    >
      <fieldset disabled={saving} className="min-w-0 space-y-4">
        <FormSection title="Source">
          <Field id={`${id}-connection`} label="GitHub connection">
            <Select
              id={`${id}-connection`}
              value={draft.connectionId || null}
              options={available.map((item) => ({
                value: item.id,
                label: item.name,
              }))}
              onValueChange={(connectionId) => {
                change({
                  connectionId,
                  repositoryId: "",
                  branch: "",
                  workflowId: "",
                  workflowRef: "",
                  workflowCommit: undefined,
                  inheritRunnerDefault: false,
                  runnerLabel: "",
                  runnerGroup: "",
                  runnerGroupId: "",
                });
              }}
            />
          </Field>
          <Field id={`${id}-repo`} label="Repository">
            <Select
              id={`${id}-repo`}
              value={draft.repositoryId || null}
              disabled={!draft.connectionId || repositoryChoices.loading}
              placeholder={
                repositoryChoices.loading
                  ? "Loading repositories…"
                  : "Select repository"
              }
              options={visibleRepos.map((item) => ({
                value: item.id,
                label: item.name,
              }))}
              onValueChange={(repositoryId) => {
                const selection = visibleRepos.find(
                  (item) => item.id === repositoryId,
                );
                change({
                  repositoryId,
                  branch: selection?.defaultBranch ?? "",
                  workflowId: "",
                  workflowRef: selection?.defaultBranch ?? "",
                  workflowCommit: undefined,
                  inheritRunnerDefault: false,
                  runnerLabel: "",
                  runnerGroup: "",
                  runnerGroupId: "",
                });
              }}
            />
            <ChoiceError
              error={repositoryChoices.error}
              onRetry={repositoryChoices.retry}
            />
            {draft.connectionId &&
              !repositoryChoices.loading &&
              !repositoryChoices.error &&
              visibleRepos.length === 0 && (
                <p className="text-xs text-muted-foreground">
                  Grant this connection access to a repository in GitHub.
                </p>
              )}
          </Field>
          <Field id={`${id}-branch`} label="Source branch">
            <Input
              id={`${id}-branch`}
              required
              value={draft.branch}
              onChange={(event) => change({ branch: event.target.value })}
            />
          </Field>
          <WorkflowFields
            id={id}
            draft={draft}
            repo={repo}
            workflows={workflows}
            automaticRefConflict={automaticRefConflict}
            change={change}
          />
        </FormSection>
        <FormSection title="Build">
          <Field id={`${id}-dockerfile`} label="Dockerfile">
            <Input
              id={`${id}-dockerfile`}
              required
              value={draft.dockerfile}
              onChange={(event) => change({ dockerfile: event.target.value })}
            />
          </Field>
          <Field id={`${id}-context`} label="Build context">
            <Input
              id={`${id}-context`}
              required
              value={draft.buildContext}
              onChange={(event) => change({ buildContext: event.target.value })}
            />
          </Field>
          <Field
            id={`${id}-runner`}
            label="Runner label and group"
            description="A label shared by several eligible Runners selects that pool."
          >
            <Select
              id={`${id}-runner`}
              value={selectedKey}
              disabled={!repo || runnerChoices.loading}
              placeholder={
                runnerChoices.loading
                  ? "Checking Runner eligibility…"
                  : "Select a Runner pool"
              }
              options={[
                ...(defaultRunner || draft.inheritRunnerDefault
                  ? [
                      {
                        value: "default",
                        label: defaultRunner
                          ? `Repository default · ${defaultRunner.label}`
                          : "Repository default",
                      },
                    ]
                  : []),
                ...runners.map((item) => ({
                  value: runnerKey(item),
                  label: item.group
                    ? `${item.label} · ${item.group}`
                    : item.label,
                })),
              ]}
              onValueChange={(key) => {
                const runner = runners.find((item) => runnerKey(item) === key);
                change({
                  inheritRunnerDefault: key === "default",
                  runnerLabel: runner?.label ?? "",
                  runnerGroup: runner?.group ?? "",
                  runnerGroupId: runner?.groupId ?? "",
                });
              }}
            />
            <ChoiceError
              error={runnerChoices.error}
              onRetry={runnerChoices.retry}
            />
            <ChoiceError error={defaults.error} onRetry={defaults.retry} />
            {defaultRunner && (
              <p className="text-xs text-muted-foreground">
                The closest scope with a saved default is{" "}
                {defaultRunner.scopeKind}. Select a Runner pool to override it
                for this Service.
              </p>
            )}
            {draft.inheritRunnerDefault &&
              !defaultRunner &&
              !defaults.loading &&
              !defaults.error && (
                <p className="text-xs text-muted-foreground">
                  Set a repository Runner default in Settings or choose a
                  Service override.
                </p>
              )}
            {resolvedRunner ? (
              <div className="space-y-1 text-xs">
                <StatusBadge
                  status={
                    resolvedRunner.status === "busy"
                      ? "queued"
                      : resolvedRunner.status
                  }
                  label={
                    {
                      ready: "Eligible",
                      busy: "Busy · builds queue",
                      offline: "Offline",
                      unavailable: "Unavailable",
                      ineligible: "Ineligible",
                    }[resolvedRunner.status]
                  }
                />
                {resolvedRunner.detail && (
                  <p className="text-muted-foreground">
                    {resolvedRunner.detail}
                  </p>
                )}
              </div>
            ) : repo && !runnerChoices.loading && !runnerChoices.error ? (
              <p className="text-xs text-muted-foreground">
                {runners.length
                  ? "Choose a Runner pool the repository can use."
                  : "No eligible Runners are registered for this repository."}
              </p>
            ) : null}
            {runnerRegistrationHref && !runnerIsEligible && (
              <Link
                to={runnerRegistrationHref}
                className="text-xs text-primary underline underline-offset-4"
              >
                Manage Runners
              </Link>
            )}
          </Field>
        </FormSection>
        <FormSection title="Release policy">
          <div className="flex items-start gap-2">
            <Checkbox
              id={`${id}-automatic`}
              checked={draft.automatic}
              onChange={(event) => change({ automatic: event.target.checked })}
            />
            <Label htmlFor={`${id}-automatic`}>
              Deploy on pushes to the source branch
            </Label>
          </div>
          <div className="flex items-start gap-2">
            <Checkbox
              id={`${id}-approval`}
              checked={draft.requireApproval}
              onChange={(event) =>
                change({ requireApproval: event.target.checked })
              }
            />
            <Label htmlFor={`${id}-approval`}>
              Require approval after a successful build
            </Label>
          </div>
        </FormSection>
      </fieldset>
      {saveError && (
        <p role="alert" className="text-sm text-destructive">
          {saveError}
        </p>
      )}
      <FormActions
        dirty={dirty}
        onDiscard={onCancel}
        className={`${editorFooterClassName} flex flex-wrap justify-end gap-2`}
      >
        <Button
          type="button"
          variant="outline"
          disabled={saving}
          onClick={onCancel}
        >
          Cancel
        </Button>
        <Button
          type="submit"
          disabled={!complete || saving || Boolean(binding && !dirty)}
        >
          {saving ? "Saving…" : "Save GitHub deployment"}
        </Button>
      </FormActions>
    </form>
  );
}
