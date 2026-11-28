import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { ChoiceError, Field } from "./binding-fields";
import { gitHubUrl } from "./github-url";
import type {
  GitDeliveryBinding,
  GitHubRepository,
  GitHubWorkflow,
} from "./types";

type WorkflowDraft = Pick<
  GitDeliveryBinding,
  "workflowId" | "workflowRef" | "workflowCommit"
>;

type Props = {
  id: string;
  draft: WorkflowDraft;
  repo?: GitHubRepository;
  workflows: {
    values?: readonly GitHubWorkflow[];
    error?: string;
    loading: boolean;
    retry: () => void;
  };
  automaticRefConflict: boolean;
  change: (changes: Partial<WorkflowDraft>) => void;
};

export function WorkflowFields({
  id,
  draft,
  repo,
  workflows,
  automaticRefConflict,
  change,
}: Props) {
  const selectedWorkflow = workflows.values?.find(
    (workflow) => workflow.id === draft.workflowId,
  );
  const reviewedRef = draft.workflowCommit || draft.workflowRef;
  const workflowReviewUrl =
    repo && selectedWorkflow && reviewedRef
      ? gitHubUrl(
          `https://github.com/${repo.name.split("/").map(encodeURIComponent).join("/")}/blob/${encodeURIComponent(reviewedRef)}/${selectedWorkflow.path.split("/").map(encodeURIComponent).join("/")}`,
        )
      : undefined;
  return (
    <>
      <Field
        id={`${id}-workflow`}
        label={
          workflows.values?.length || workflows.loading
            ? "GitHub workflow"
            : "GitHub workflow ID"
        }
        description={
          workflows.values?.length || workflows.loading
            ? undefined
            : "Enter the numeric ID of the reviewed Actions workflow."
        }
      >
        {workflows.values?.length || workflows.loading ? (
          <Select
            id={`${id}-workflow`}
            value={draft.workflowId || null}
            disabled={workflows.loading}
            placeholder={
              workflows.loading
                ? "Loading GitHub workflows…"
                : "Select a workflow"
            }
            options={(workflows.values ?? []).map((workflow) => ({
              value: workflow.id,
              label: `${workflow.name} · ${workflow.path.split("/").at(-1)}`,
            }))}
            onValueChange={(workflowId) => {
              change({
                workflowId,
                workflowCommit: undefined,
              });
            }}
          />
        ) : (
          <Input
            id={`${id}-workflow`}
            aria-describedby={`${id}-workflow-help`}
            required
            inputMode="numeric"
            pattern="[1-9][0-9]*"
            value={draft.workflowId}
            onChange={(event) =>
              change({
                workflowId: event.target.value,
                workflowCommit: undefined,
              })
            }
          />
        )}
        <ChoiceError error={workflows.error} onRetry={workflows.retry} />
        {workflowReviewUrl && (
          <a
            href={workflowReviewUrl}
            target="_blank"
            rel="noreferrer"
            className="text-xs text-primary underline underline-offset-4"
          >
            Review workflow on GitHub
          </a>
        )}
      </Field>
      <Field
        id={`${id}-workflow-ref`}
        label="Workflow ref"
        description="Use a protected ref for the approved workflow. Automatic deployment requires a separate ref from the source branch."
      >
        <Input
          id={`${id}-workflow-ref`}
          aria-describedby={`${id}-workflow-ref-help`}
          aria-invalid={automaticRefConflict || undefined}
          required
          value={draft.workflowRef}
          onChange={(event) =>
            change({
              workflowRef: event.target.value,
              workflowCommit: undefined,
            })
          }
        />
        {automaticRefConflict && (
          <p role="alert" className="text-xs text-destructive">
            Choose a separate protected workflow ref for automatic deployment.
          </p>
        )}
      </Field>
      <Field
        id={`${id}-workflow-commit`}
        label="Reviewed workflow commit (optional)"
        description="Enter the 40-character commit SHA of the workflow you reviewed."
      >
        <Input
          id={`${id}-workflow-commit`}
          aria-describedby={`${id}-workflow-commit-help`}
          pattern="[a-fA-F0-9]{40}"
          spellCheck={false}
          value={draft.workflowCommit ?? ""}
          onChange={(event) => change({ workflowCommit: event.target.value })}
        />
      </Field>
    </>
  );
}
