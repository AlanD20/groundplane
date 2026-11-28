import { useEffect, useId, useState, type FormEvent } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { listAllServices } from "@/features/service/api";
import type { Service } from "@/lib/types";
import { newULID } from "@/lib/utils";
import type { GrantAction, GrantScope, GrantWrite } from "./cli-grant-api";

export const operationLabels: Record<GrantAction["operation"], string> = {
  "service.read": "Read a Service",
  "task.read": "Read a Task",
  "service.image.fetch": "Fetch a Service image",
  "service.deploy": "Deploy a Service image",
};
const operations = Object.entries(operationLabels).map(([value, label]) => ({
  value,
  label,
}));
const isWrite = (operation: GrantAction["operation"]) =>
  operation === "service.image.fetch" || operation === "service.deploy";
const taskPattern = /^task_[0-9A-HJKMNP-TV-Z]{26}$/;
const imagePattern =
  /^registry\.groundplane\.internal:5000\/[^@\s]+@sha256:[a-f0-9]{64}$/;

type Draft = GrantAction & { id: string; tasks: string };
export type EnvironmentChoice = {
  value: string;
  label: string;
  scope: GrantScope;
};

function row(action?: GrantAction): Draft {
  return {
    operation: "service.read",
    target_id: "",
    ...action,
    id: newULID(),
    tasks: action?.task_ids?.join(", ") ?? "",
  };
}

function approval(draft: Draft): GrantAction {
  const target = draft.target_id.trim();
  if (draft.operation === "task.read" && !taskPattern.test(target))
    throw new Error("Enter a valid Task ID.");
  if (draft.operation !== "task.read" && !target)
    throw new Error("Choose a Service for every Service action.");
  const action = { operation: draft.operation, target_id: target };
  if (!isWrite(draft.operation)) return action;
  const image = draft.image?.trim();
  if (!image || !imagePattern.test(image))
    throw new Error(
      "Enter a managed registry image pinned to a SHA-256 digest.",
    );
  const tasks = draft.tasks.split(/[\s,]+/).filter(Boolean);
  if (
    tasks.length > 16 ||
    new Set(tasks).size !== tasks.length ||
    tasks.some((id) => !taskPattern.test(id))
  ) {
    throw new Error(
      "Enter up to 16 unique prerequisite Task IDs for each action.",
    );
  }
  return { ...action, image, task_ids: tasks.length ? tasks : undefined };
}

export function CLIGrantFields({
  choices,
  initialScope,
  initialActions,
  disabled,
  locked,
  onCancel,
  onSubmit,
}: {
  choices: readonly EnvironmentChoice[];
  initialScope?: string;
  initialActions: readonly GrantAction[];
  disabled: boolean;
  locked: boolean;
  onCancel: () => void;
  onSubmit: (body: GrantWrite) => void;
}) {
  const id = useId();
  const [environment, setEnvironment] = useState(
    initialScope ?? (choices.length === 1 ? choices[0].value : ""),
  );
  const [drafts, setDrafts] = useState(() =>
    initialActions.length ? initialActions.map(row) : [row()],
  );
  const [duration, setDuration] = useState("900");
  const [services, setServices] = useState<Service[]>();
  const [serviceError, setServiceError] = useState("");
  const [attempt, setAttempt] = useState(0);
  const [validation, setValidation] = useState("");
  const selected = choices.find((item) => item.value === environment);
  const needsServices = drafts.some(
    (action) => action.operation !== "task.read",
  );

  useEffect(() => {
    if (!environment || !needsServices) return;
    const controller = new AbortController();
    const { signal } = controller;
    void listAllServices(environment, signal).then(
      (items) => {
        if (signal.aborted) return;
        setServices(items);
        setServiceError("");
      },
      (cause) => {
        if (!signal.aborted)
          setServiceError(
            cause instanceof Error ? cause.message : "Could not load Services.",
          );
      },
    );
    return () => controller.abort();
  }, [attempt, environment, needsServices]);

  function change(id: string, values: Partial<Draft>) {
    setValidation("");
    setDrafts((current) =>
      current.map((item) => (item.id === id ? { ...item, ...values } : item)),
    );
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selected || disabled) return;
    try {
      const actions = drafts.map(approval);
      if (
        actions.some(
          (action) =>
            action.operation !== "task.read" &&
            !services?.some((service) => service.id === action.target_id),
        )
      ) {
        throw new Error(
          "Choose a Service in the selected Environment for every Service action.",
        );
      }
      if (
        new Set(
          actions.map((action) =>
            JSON.stringify([action.operation, action.target_id]),
          ),
        ).size !== actions.length
      ) {
        throw new Error("Choose each operation and target combination once.");
      }
      const lifetime = Number(duration);
      if (!Number.isInteger(lifetime) || lifetime < 1 || lifetime > 3600)
        throw new Error("Enter a duration between 1 and 3,600 seconds.");
      onSubmit({ scope: selected.scope, actions, lifetime_seconds: lifetime });
    } catch (cause) {
      setValidation(
        cause instanceof Error
          ? cause.message
          : "Review the requested actions.",
      );
    }
  }

  return (
    <form
      className="space-y-4 rounded-lg border border-border bg-surface p-4"
      onSubmit={submit}
    >
      <div className="grid gap-3 sm:grid-cols-2">
        <div className="space-y-2">
          <Label htmlFor={`${id}-environment`}>Environment</Label>
          <Select
            id={`${id}-environment`}
            value={selected ? environment : null}
            disabled={disabled || !choices.length}
            options={[...choices]}
            placeholder="Select Environment"
            onValueChange={(next) => {
              setEnvironment(next);
              setServices(undefined);
              setServiceError("");
              setValidation("");
              setDrafts((current) =>
                current.map((item) => ({ ...item, target_id: "", tasks: "" })),
              );
            }}
          />
          {!choices.length && (
            <p className="text-xs text-muted-foreground">
              No ready Environments are available in this Runner’s scope.
            </p>
          )}
        </div>
        <div className="space-y-2">
          <Label htmlFor={`${id}-duration`}>Grant duration (seconds)</Label>
          <Input
            id={`${id}-duration`}
            type="number"
            min={1}
            max={3600}
            required
            disabled={disabled}
            value={duration}
            onChange={(event) => setDuration(event.target.value)}
          />
        </div>
      </div>
      {serviceError && needsServices && (
        <div role="alert" className="flex flex-wrap items-center gap-2">
          <p className="text-sm text-destructive">{serviceError}</p>
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={disabled}
            onClick={() => setAttempt((value) => value + 1)}
          >
            Retry Services
          </Button>
        </div>
      )}
      <div className="space-y-3">
        {drafts.map((item, index) => (
          <fieldset
            key={item.id}
            disabled={disabled}
            className="space-y-3 rounded-lg border border-border p-3"
          >
            <legend className="px-1 text-xs font-medium text-muted-foreground">
              Action {index + 1}
            </legend>
            <div className="grid items-start gap-3 sm:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor={`${item.id}-operation`}>Operation</Label>
                <Select
                  id={`${item.id}-operation`}
                  value={item.operation}
                  disabled={disabled}
                  options={operations}
                  onValueChange={(value) => {
                    const operation = value as GrantAction["operation"];
                    const switchesTarget =
                      (operation === "task.read") !==
                      (item.operation === "task.read");
                    change(item.id, {
                      operation,
                      target_id: switchesTarget ? "" : item.target_id,
                    });
                  }}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor={`${item.id}-target`}>
                  {item.operation === "task.read" ? "Task ID" : "Service"}
                </Label>
                {item.operation === "task.read" ? (
                  <Input
                    id={`${item.id}-target`}
                    required
                    spellCheck={false}
                    className="font-mono"
                    value={item.target_id}
                    onChange={(event) =>
                      change(item.id, { target_id: event.target.value })
                    }
                  />
                ) : (
                  <Select
                    id={`${item.id}-target`}
                    value={item.target_id || null}
                    disabled={disabled || !services?.length}
                    placeholder={
                      selected && !services && !serviceError
                        ? "Loading Services…"
                        : "Select Service"
                    }
                    options={(services ?? []).map((service) => ({
                      value: service.id,
                      label: service.name,
                    }))}
                    onValueChange={(target_id) =>
                      change(item.id, { target_id })
                    }
                  />
                )}
              </div>
            </div>
            {isWrite(item.operation) && (
              <>
                <div className="space-y-2">
                  <Label htmlFor={`${item.id}-image`}>
                    Managed image digest
                  </Label>
                  <Input
                    id={`${item.id}-image`}
                    required
                    maxLength={512}
                    spellCheck={false}
                    className="font-mono"
                    placeholder="registry.groundplane.internal:5000/repository@sha256:…"
                    value={item.image ?? ""}
                    onChange={(event) =>
                      change(item.id, { image: event.target.value })
                    }
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor={`${item.id}-tasks`}>
                    Prerequisite Task IDs (optional)
                  </Label>
                  <Input
                    id={`${item.id}-tasks`}
                    spellCheck={false}
                    value={item.tasks}
                    onChange={(event) =>
                      change(item.id, { tasks: event.target.value })
                    }
                  />
                  <p className="text-xs text-muted-foreground">
                    Enter comma-separated IDs from this Environment.
                  </p>
                </div>
              </>
            )}
            {drafts.length > 1 && (
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={() =>
                  setDrafts((current) =>
                    current.filter((action) => action.id !== item.id),
                  )
                }
              >
                Remove action {index + 1}
              </Button>
            )}
          </fieldset>
        ))}
      </div>
      <Button
        type="button"
        size="sm"
        variant="outline"
        disabled={disabled || drafts.length >= 16}
        onClick={() => setDrafts((current) => [...current, row()])}
      >
        Add action
      </Button>
      {validation && (
        <p role="alert" className="text-sm text-destructive">
          {validation}
        </p>
      )}
      <div className="flex flex-wrap justify-end gap-2">
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={locked}
          onClick={onCancel}
        >
          Cancel
        </Button>
        <Button
          type="submit"
          size="sm"
          disabled={
            disabled ||
            !selected ||
            (needsServices && (!services || Boolean(serviceError)))
          }
        >
          Save grant
        </Button>
      </div>
    </form>
  );
}
