import { useId, useRef, useState, type FormEvent, type ReactNode } from "react";
import { Box } from "lucide-react";
import { useParams } from "react-router-dom";
import { ResourcePanel } from "@/components/common/resource-panel";
import { TaskLink } from "@/components/common/task-link";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, type SelectOption } from "@/components/ui/select";
import type {
  BlueprintActions,
  BlueprintValidationResponse,
} from "@/features/blueprint/api";
import { BlueprintReview } from "@/features/blueprint/blueprint-review";
import {
  createBlueprintTextApplyRequest,
  type BlueprintApplyRequest,
} from "@/lib/blueprint-bundle";
import type { Environment } from "@/lib/types";
import { newULID } from "@/lib/utils";
import {
  firstDeploymentBlueprint,
  type FirstDeploymentDraft,
} from "./first-deployment-model";
import { FirstDeploymentServiceConfiguration } from "./first-deployment-service-configuration";
import { FirstDeploymentStorageMigrations } from "./first-deployment-storage-migrations";
import { FirstDeploymentAttachments } from "./first-deployment-attachments";
import type { FirstDeploymentBackingOption } from "./first-deployment-attachment-model";

type WizardEnvironment = Pick<
  Environment,
  | "id"
  | "name"
  | "networkPool"
  | "provisioningState"
  | "deletionTaskId"
  | "createTaskId"
> & {
  zones: Pick<Environment["zones"][number], "id" | "name" | "subnet">[];
  services: Pick<Environment["services"][number], "id" | "name" | "image">[];
  volumes?: Environment["volumes"];
};

export type FirstDeploymentProject = {
  id: string;
  label: string;
  environments?: WizardEnvironment[];
  secretOptions?: SelectOption[];
  backingOptions?: FirstDeploymentBackingOption[];
};

type TaskAccepted = { task_id: string };

export type FirstDeploymentWizardProps = {
  projects: FirstDeploymentProject[];
  initialProjectId?: string;
  initialEnvironmentId?: string;
  environmentLocked?: boolean;
  blueprint: BlueprintActions;
  renderRecoveryReview?: (
    environmentId: string,
    draft: FirstDeploymentDraft,
    environmentPath?: string,
  ) => ReactNode;
  onCreateEnvironment: (
    projectId: string,
    name: string,
    networkPool: string,
  ) => Promise<TaskAccepted>;
  onFetchImage: (
    image: string,
    idempotencyKey: string,
  ) => Promise<TaskAccepted>;
};

type Prepared = {
  environmentId: string;
  document: string;
  revision: string;
  request: BlueprintApplyRequest;
  validation: BlueprintValidationResponse;
  key: string;
};

const create = "__create__";
const errorClass =
  "rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-sm text-destructive";

const initialDraft: FirstDeploymentDraft = {
  service: "",
  image: "",
  port: "8080",
  healthPath: "/health",
  zone: "application",
  subnet: "",
  newService: true,
  newZone: true,
  entries: [],
  volumes: [],
};

export function FirstDeploymentWizard({
  projects,
  initialProjectId,
  initialEnvironmentId,
  environmentLocked = false,
  blueprint,
  renderRecoveryReview,
  onCreateEnvironment,
  onFetchImage,
}: FirstDeploymentWizardProps) {
  const prefix = useId();
  const params = useParams<"tenant" | "project">();
  const [projectId, setProjectId] = useState(initialProjectId ?? "");
  const [environmentId, setEnvironmentId] = useState(
    initialEnvironmentId ?? "",
  );
  const [newName, setNewName] = useState("");
  const [newPool, setNewPool] = useState("");
  const [created, setCreated] = useState<{
    projectId: string;
    name: string;
    pool: string;
    taskId: string;
  }>();
  const [draft, setDraft] = useState(initialDraft);
  const [fetched, setFetched] = useState<{ image: string; taskId: string }>();
  const [prepared, setPrepared] = useState<Prepared>();
  const [taskId, setTaskId] = useState<string>();
  const [working, setWorking] = useState<
    "environment" | "fetch" | "review" | "apply"
  >();
  const [error, setError] = useState("");
  const busy = useRef(false);
  const fetchIntent = useRef<{ image: string; key: string }>(undefined);
  const project = projects.find((candidate) => candidate.id === projectId);
  const environments = project?.environments ?? [];
  const createdEnvironment =
    created?.projectId === projectId
      ? environments.find(
          (candidate) =>
            candidate.name === created.name &&
            candidate.networkPool === created.pool &&
            (candidate.createTaskId === created.taskId ||
              candidate.provisioningState === "ready"),
        )
      : undefined;
  const selectedEnvironment =
    environmentId === create
      ? createdEnvironment
      : environments.find((candidate) => candidate.id === environmentId);
  const environmentPath =
    params.tenant &&
    params.project &&
    projectId === initialProjectId &&
    selectedEnvironment
      ? `/t/${encodeURIComponent(params.tenant)}/${encodeURIComponent(params.project)}/${encodeURIComponent(selectedEnvironment.name)}`
      : undefined;
  const ready =
    selectedEnvironment?.provisioningState === "ready" &&
    !selectedEnvironment.deletionTaskId;
  const applicableFetch =
    fetched?.image === draft.image.trim() ? fetched : undefined;

  function updateDraft(update: Partial<FirstDeploymentDraft>) {
    setDraft((current) => ({ ...current, ...update }));
    setPrepared(undefined);
    setTaskId(undefined);
    setError("");
  }

  function selectEnvironment(value: string) {
    setEnvironmentId(value);
    setCreated(undefined);
    updateDraft({
      service: "",
      newService: true,
      zone: "application",
      newZone: true,
      subnet: "",
      entries: [],
      volumes: [],
      migration: undefined,
      attachments: [],
    });
  }

  async function perform(
    action: NonNullable<typeof working>,
    operation: () => Promise<void>,
  ) {
    if (busy.current) return;
    busy.current = true;
    setWorking(action);
    setError("");
    try {
      await operation();
    } catch (cause) {
      setError(
        cause instanceof Error
          ? cause.message
          : "The request could not be completed.",
      );
    } finally {
      busy.current = false;
      setWorking(undefined);
    }
  }

  function createEnvironment() {
    const name = newName.trim();
    const pool = newPool.trim();
    if (!project || !name || !pool) return;
    void perform("environment", async () => {
      if (environments.some((candidate) => candidate.name === name))
        throw new Error(
          "This Project already has an Environment with that name. Select it to continue.",
        );
      const result = await onCreateEnvironment(project.id, name, pool);
      setCreated({ projectId: project.id, name, pool, taskId: result.task_id });
    });
  }

  function fetchImage() {
    const image = draft.image.trim();
    if (!image || applicableFetch) return;
    if (fetchIntent.current?.image !== image)
      fetchIntent.current = { image, key: newULID() };
    const { key } = fetchIntent.current;
    void perform("fetch", async () => {
      const result = await onFetchImage(image, key);
      setFetched({ image, taskId: result.task_id });
      setPrepared(undefined);
    });
  }

  function review(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedEnvironment || !ready) return;
    const id = selectedEnvironment.id;
    void perform("review", async () => {
      setPrepared(undefined);
      const current = await blueprint.getBlueprint(id);
      const document = firstDeploymentBlueprint(
        current.document,
        draft,
        project?.backingOptions,
      );
      const request = await createBlueprintTextApplyRequest(document);
      if (
        current.revision !== "0" &&
        !(draft.newService && draft.newZone) &&
        !draft.entries.length &&
        !draft.attachments?.length &&
        !draft.volumes.some((volume) => !volume.existing) &&
        !draft.migration
      )
        request.manifest.service = draft.service.trim();
      if (applicableFetch) request.manifest.after = [applicableFetch.taskId];
      const validation = await blueprint.validateBlueprint(
        id,
        request,
        current.revision,
      );
      const changes = validation.changes ?? [];
      if (
        changes.some(
          (change) =>
            change.action === "remove" ||
            (change.resource === "service" &&
              change.action === "create" &&
              change.key !== draft.service.trim()),
        )
      )
        throw new Error(
          "The proposed Blueprint adds other Services or removes resources. Use the Environment Blueprint editor to review those changes.",
        );
      setPrepared({
        environmentId: id,
        document,
        revision: current.revision,
        request,
        validation,
        key: newULID(),
      });
    });
  }

  function apply() {
    if (
      !prepared ||
      selectedEnvironment?.id !== prepared.environmentId ||
      !ready
    )
      return;
    void perform("apply", async () => {
      const result = await blueprint.applyBlueprint(
        prepared.environmentId,
        prepared.request,
        prepared.revision,
        prepared.key,
      );
      setTaskId(result.task_id);
      setPrepared(undefined);
    });
  }

  const projectOptions = projects.map((candidate) => ({
    value: candidate.id,
    label: candidate.label,
  }));
  const environmentOptions = [
    ...environments
      .filter((candidate) => !candidate.deletionTaskId)
      .map((candidate) => ({ value: candidate.id, label: candidate.name })),
    { value: create, label: "Create an Environment…" },
  ];

  return (
    <ResourcePanel
      title={
        <span className="flex items-center gap-2">
          <Box aria-hidden className="size-4" /> Deploy an image
        </span>
      }
    >
      <form onSubmit={review} className="space-y-5">
        <fieldset disabled={!!working} className="space-y-5">
          {(!environmentLocked || projectOptions.length > 1) && (
            <div className="grid gap-3 sm:grid-cols-2">
              {projectOptions.length > 1 && (
                <div className="space-y-2">
                  <Label htmlFor={`${prefix}-project`}>Project</Label>
                  <Select
                    id={`${prefix}-project`}
                    searchable
                    value={projectId || null}
                    options={projectOptions}
                    onValueChange={(value) => {
                      setProjectId(value);
                      selectEnvironment("");
                    }}
                  />
                </div>
              )}
              {!environmentLocked && (
                <div className="space-y-2">
                  <Label htmlFor={`${prefix}-environment`}>Environment</Label>
                  <Select
                    id={`${prefix}-environment`}
                    searchable
                    value={selectedEnvironment?.id ?? (environmentId || null)}
                    options={environmentOptions}
                    disabled={!project}
                    onValueChange={selectEnvironment}
                  />
                </div>
              )}
            </div>
          )}
          {project && !project.environments && (
            <p className="text-sm text-muted-foreground">
              Waiting for the Project’s Environment list.
            </p>
          )}
          {project && environmentId === create && !createdEnvironment && (
            <div className="space-y-3 rounded-lg border border-border p-3">
              <div className="grid gap-3 sm:grid-cols-2">
                <label className="space-y-2 text-sm font-medium">
                  Environment name
                  <Input
                    value={newName}
                    onChange={(event) => setNewName(event.target.value)}
                  />
                </label>
                <label className="space-y-2 text-sm font-medium">
                  Environment network pool
                  <Input
                    value={newPool}
                    placeholder="10.42.0.0/24"
                    onChange={(event) => setNewPool(event.target.value)}
                  />
                </label>
              </div>
              <Button
                type="button"
                variant="outline"
                disabled={!newName.trim() || !newPool.trim() || !!created}
                onClick={createEnvironment}
              >
                {working === "environment"
                  ? "Submitting…"
                  : "Create Environment"}
              </Button>
            </div>
          )}
          {created && (
            <p className="text-sm">
              <TaskLink taskId={created.taskId}>
                Open Environment creation Task
              </TaskLink>
            </p>
          )}
          {selectedEnvironment && !ready && (
            <p role="status" className="text-sm text-muted-foreground">
              Wait until the selected Environment is ready.
            </p>
          )}
          {ready && selectedEnvironment && (
            <>
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor={`${prefix}-service`}>Service</Label>
                  <Select
                    id={`${prefix}-service`}
                    searchable
                    value={draft.newService ? create : draft.service}
                    options={[
                      { value: create, label: "Create a Service…" },
                      ...selectedEnvironment.services.map((service) => ({
                        value: service.name,
                        label: service.name,
                      })),
                    ]}
                    onValueChange={(value) => {
                      const existing = selectedEnvironment.services.find(
                        (service) => service.name === value,
                      );
                      updateDraft({
                        newService: value === create,
                        service: existing?.name ?? "",
                        image: existing?.image ?? "",
                        entries: [],
                        volumes: [],
                        migration: undefined,
                        attachments: [],
                      });
                    }}
                  />
                </div>
                {draft.newService && (
                  <label className="space-y-2 text-sm font-medium">
                    Service name
                    <Input
                      value={draft.service}
                      onChange={(event) =>
                        updateDraft({ service: event.target.value })
                      }
                      required
                    />
                  </label>
                )}
              </div>
              <div className="space-y-2">
                <Label htmlFor={`${prefix}-image`}>OCI image</Label>
                <div className="flex flex-wrap gap-2">
                  <Input
                    id={`${prefix}-image`}
                    className="min-w-48 flex-1"
                    value={draft.image}
                    placeholder="registry.example.com/team/web:1.0.0"
                    onChange={(event) =>
                      updateDraft({ image: event.target.value })
                    }
                    required
                  />
                  <Button
                    type="button"
                    variant="outline"
                    disabled={!draft.image.trim() || !!applicableFetch}
                    onClick={fetchImage}
                  >
                    {working === "fetch" ? "Submitting…" : "Fetch image"}
                  </Button>
                </div>
                <p className="text-xs text-muted-foreground">
                  Fetch the image first if it is not already present on the
                  Host.
                </p>
                {applicableFetch && (
                  <TaskLink taskId={applicableFetch.taskId}>
                    Open image Fetch Task
                  </TaskLink>
                )}
              </div>
              <FirstDeploymentServiceConfiguration
                draft={draft}
                environment={selectedEnvironment}
                secretOptions={project?.secretOptions}
                onChange={updateDraft}
              />
              <FirstDeploymentStorageMigrations
                draft={draft}
                volumes={selectedEnvironment.volumes ?? []}
                environmentPath={environmentPath}
                disabled={Boolean(working)}
                onChange={updateDraft}
              />
              {draft.newService && (
                <FirstDeploymentAttachments
                  attachments={draft.attachments ?? []}
                  options={project?.backingOptions ?? []}
                  onChange={(attachments) => updateDraft({ attachments })}
                />
              )}
            </>
          )}
        </fieldset>
        {error && (
          <p role="alert" className={errorClass}>
            {error}
          </p>
        )}
        {prepared && (
          <div className="space-y-3 rounded-lg border border-border p-4">
            <h3 className="text-sm font-medium">
              Review Blueprint and exposure
            </h3>
            <p className="text-sm text-muted-foreground">
              Existing public Routes stay as configured. This deployment adds no
              public listener or Route.
            </p>
            {applicableFetch && (
              <TaskLink taskId={applicableFetch.taskId}>
                Required image Fetch Task
              </TaskLink>
            )}
            <BlueprintReview validation={prepared.validation} />
            {renderRecoveryReview?.(
              prepared.environmentId,
              draft,
              environmentPath,
            )}
            <details>
              <summary className="cursor-pointer text-sm font-medium">
                Proposed Blueprint
              </summary>
              <pre className="mt-2 max-h-96 overflow-auto rounded-md bg-muted p-3 text-xs">
                {prepared.document}
              </pre>
            </details>
          </div>
        )}
        {taskId && (
          <p role="status" className="text-sm">
            Deployment accepted.{" "}
            <TaskLink taskId={taskId}>Open deployment Task</TaskLink>
          </p>
        )}
        <div className="flex flex-wrap justify-end gap-2">
          <Button
            type="submit"
            variant="outline"
            disabled={
              !ready ||
              !draft.service.trim() ||
              !draft.image.trim() ||
              !!working ||
              !!taskId
            }
          >
            {working === "review" ? "Validating…" : "Review deployment"}
          </Button>
          {prepared && (
            <Button
              type="button"
              disabled={!!working || !ready}
              onClick={apply}
            >
              {working === "apply" ? "Submitting…" : "Apply deployment"}
            </Button>
          )}
        </div>
      </form>
    </ResourcePanel>
  );
}
