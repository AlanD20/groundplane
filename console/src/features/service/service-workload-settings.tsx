import { editorFooterClassName } from "@/components/common/workspace-section";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { useStore } from "@/lib/store";
import type {
  Environment,
  Service,
  ServiceDependency,
  ServiceDependencyCondition,
  ServiceDependencyPhase,
} from "@/lib/types";
import { Plus, RotateCcw, Trash2 } from "lucide-react";
import { useState } from "react";

type WorkloadSettingsKind = "process" | "relationships";

const dependencyPhases: {
  value: ServiceDependencyPhase;
  label: string;
}[] = [
  { value: "start", label: "Start" },
  { value: "deploy", label: "Deploy" },
  { value: "rollback", label: "Rollback" },
  { value: "always", label: "Always" },
];

export function ServiceWorkloadSettingsForm({
  env,
  service,
  kind,
  onSaved,
  onClose,
}: {
  env: Environment;
  service: Service;
  kind: WorkloadSettingsKind;
  onSaved?: () => void;
  onClose: () => void;
}) {
  const store = useStore();
  const [command, setCommand] = useState([...service.command]);
  const [entrypoint, setEntrypoint] = useState([...service.entrypoint]);
  const [workingDir, setWorkingDir] = useState(service.workingDir);
  const [user, setUser] = useState(service.user);
  const [maxSize, setMaxSize] = useState(service.logging.maxSize);
  const [maxFile, setMaxFile] = useState(
    service.logging.maxFile > 0 ? String(service.logging.maxFile) : "",
  );
  const [aliases, setAliases] = useState<Record<string, string[]>>(() =>
    Object.fromEntries(
      Object.entries(service.aliasesByZone).map(([zone, values]) => [
        zone,
        [...values],
      ]),
    ),
  );
  const [dependencies, setDependencies] = useState<
    Record<string, ServiceDependency>
  >(() =>
    Object.fromEntries(
      Object.entries(service.dependencies).map(([name, dependency]) => [
        name,
        { ...dependency, phases: [...dependency.phases] },
      ]),
    ),
  );
  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const parsedMaxFile = maxFile.trim() === "" ? 0 : Number(maxFile);
  const maxFileInvalid = !Number.isInteger(parsedMaxFile) || parsedMaxFile < 0;
  const aliasesInvalid = Object.values(aliases).some((values) =>
    values.some((value) => value === ""),
  );
  const canSubmit = !submitting && !maxFileInvalid && !aliasesInvalid;

  async function save() {
    if (!canSubmit) return;
    setSubmitting(true);
    setSubmitError(null);
    try {
      const current = await store.getService(service.id);
      await store.updateService(env.id, service.id, {
        ...current,
        ...(kind === "process"
          ? {
              command,
              entrypoint,
              workingDir,
              user,
              logging: {
                maxSize,
                maxFile: parsedMaxFile,
              },
            }
          : {
              aliasesByZone: Object.fromEntries(
                Object.entries(aliases).filter(
                  ([, values]) => values.length > 0,
                ),
              ),
              dependencies,
            }),
      });
      onSaved?.();
      onClose();
    } catch (error) {
      setSubmitError(
        error instanceof Error ? error.message : "Service mutation failed",
      );
    } finally {
      setSubmitting(false);
    }
  }

  const aliasZones = Array.from(
    new Set([...service.zones, ...Object.keys(aliases)]),
  );
  const dependencyNames = Object.keys(dependencies);
  const availableDependencies = env.services
    .filter(
      (candidate) =>
        candidate.id !== service.id &&
        !dependencyNames.includes(candidate.name),
    )
    .map((candidate) => candidate.name);

  return (
    <form
      className="space-y-5"
      onSubmit={(event) => {
        event.preventDefault();
        void save();
      }}
    >
      <fieldset disabled={submitting} className="contents">
        {kind === "process" ? (
          <div className="space-y-5">
            <ArgumentList
              id="service-command"
              label="Command arguments"
              description="Each item is passed as one literal argument. Reset to use the image CMD."
              values={command}
              onChange={setCommand}
            />
            <ArgumentList
              id="service-entrypoint"
              label="Entrypoint arguments"
              description="Each item is passed as one literal argument. Reset to use the image ENTRYPOINT."
              values={entrypoint}
              onChange={setEntrypoint}
            />
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="service-working-directory">
                  Working directory
                </Label>
                <Input
                  id="service-working-directory"
                  value={workingDir}
                  onChange={(event) => setWorkingDir(event.target.value)}
                  placeholder="Image default"
                />
                <p className="text-xs text-muted-foreground">
                  Leave empty to use the image working directory.
                </p>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="self-start"
                  disabled={workingDir === ""}
                  onClick={() => setWorkingDir("")}
                >
                  <RotateCcw /> Reset to image default
                </Button>
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="service-container-user">Container user</Label>
                <Input
                  id="service-container-user"
                  value={user}
                  onChange={(event) => setUser(event.target.value)}
                  placeholder="Image default"
                />
                <p className="text-xs text-muted-foreground">
                  Leave empty to use the image user.
                </p>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  className="self-start"
                  disabled={user === ""}
                  onClick={() => setUser("")}
                >
                  <RotateCcw /> Reset to image default
                </Button>
              </div>
            </div>
            <fieldset className="rounded-lg border border-border p-3">
              <legend className="px-1 text-sm font-medium">Log rotation</legend>
              <p className="mb-3 text-xs text-muted-foreground">
                Empty values use the runtime defaults.
              </p>
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="service-log-max-size">
                    Maximum file size
                  </Label>
                  <Input
                    id="service-log-max-size"
                    value={maxSize}
                    onChange={(event) => setMaxSize(event.target.value)}
                    placeholder="10m"
                  />
                </div>
                <div className="flex flex-col gap-1.5">
                  <Label htmlFor="service-log-max-files">Maximum files</Label>
                  <Input
                    id="service-log-max-files"
                    type="number"
                    min={0}
                    step={1}
                    value={maxFile}
                    aria-invalid={maxFileInvalid || undefined}
                    aria-describedby={
                      maxFileInvalid ? "service-log-max-files-error" : undefined
                    }
                    onChange={(event) => setMaxFile(event.target.value)}
                    placeholder="Runtime default"
                  />
                  {maxFileInvalid && (
                    <p
                      id="service-log-max-files-error"
                      className="text-xs text-destructive"
                      role="alert"
                    >
                      Enter a whole number of zero or more.
                    </p>
                  )}
                </div>
              </div>
            </fieldset>
          </div>
        ) : (
          <div className="space-y-5">
            <fieldset className="space-y-4">
              <legend className="text-sm font-medium">Network aliases</legend>
              <p className="text-xs text-muted-foreground">
                Aliases belong to a joined Zone and are saved as literal names.
              </p>
              {aliasZones.length > 0 ? (
                aliasZones.map((zone) => (
                  <ArgumentList
                    key={zone}
                    id={`service-alias-${zone}`}
                    label={zoneLabel(env, zone)}
                    description=""
                    itemName="alias"
                    emptyLabel="No aliases"
                    resetLabel="Remove all aliases"
                    requireNonEmpty
                    values={aliases[zone] ?? []}
                    onChange={(values) =>
                      setAliases((current) => ({
                        ...current,
                        [zone]: values,
                      }))
                    }
                  />
                ))
              ) : (
                <p className="rounded-lg border border-border p-3 text-sm text-muted-foreground">
                  Join a Zone before adding a network alias.
                </p>
              )}
            </fieldset>
            <fieldset className="space-y-3">
              <legend className="text-sm font-medium">Dependencies</legend>
              <p className="text-xs text-muted-foreground">
                Choose another Service, its required state and the lifecycle
                phases that enforce the dependency. With no phases selected, the
                dependency applies to native startup ordering only.
              </p>
              {dependencyNames.map((name, index) => (
                <DependencyEditor
                  key={name}
                  index={index}
                  name={name}
                  dependency={dependencies[name]}
                  serviceNames={Array.from(
                    new Set([
                      name,
                      ...env.services
                        .filter(
                          (candidate) =>
                            candidate.id !== service.id &&
                            (!dependencyNames.includes(candidate.name) ||
                              candidate.name === name),
                        )
                        .map((candidate) => candidate.name),
                    ]),
                  )}
                  onNameChange={(nextName) =>
                    setDependencies((current) => {
                      const next = { ...current };
                      delete next[name];
                      next[nextName] = current[name];
                      return next;
                    })
                  }
                  onChange={(dependency) =>
                    setDependencies((current) => ({
                      ...current,
                      [name]: dependency,
                    }))
                  }
                  onRemove={() =>
                    setDependencies((current) => {
                      const next = { ...current };
                      delete next[name];
                      return next;
                    })
                  }
                />
              ))}
              {dependencyNames.length === 0 && (
                <p className="rounded-lg border border-border p-3 text-sm text-muted-foreground">
                  No Service dependencies.
                </p>
              )}
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={availableDependencies.length === 0}
                onClick={() => {
                  const name = availableDependencies[0];
                  if (!name) return;
                  setDependencies((current) => ({
                    ...current,
                    [name]: {
                      condition: "service_healthy",
                      phases: [],
                    },
                  }));
                }}
              >
                <Plus /> Add dependency
              </Button>
            </fieldset>
          </div>
        )}
        {submitError && (
          <p className="text-sm text-destructive" role="alert">
            {submitError}
          </p>
        )}
        <div
          className={`${editorFooterClassName} sticky bottom-0 flex justify-end gap-2 pb-1`}
        >
          <Button
            type="button"
            variant="outline"
            disabled={submitting}
            onClick={onClose}
          >
            Cancel
          </Button>
          <Button type="submit" disabled={!canSubmit}>
            {submitting ? "Saving…" : "Save desired"}
          </Button>
        </div>
      </fieldset>
    </form>
  );
}

function ArgumentList({
  id,
  label,
  description,
  values,
  onChange,
  itemName = "argument",
  emptyLabel = "Image default",
  resetLabel = "Reset to image default",
  requireNonEmpty = false,
}: {
  id: string;
  label: string;
  description: string;
  values: string[];
  onChange: (values: string[]) => void;
  itemName?: string;
  emptyLabel?: string;
  resetLabel?: string;
  requireNonEmpty?: boolean;
}) {
  const hasEmptyValue = requireNonEmpty && values.some((value) => value === "");
  return (
    <fieldset className="rounded-lg border border-border p-3">
      <legend className="px-1 text-sm font-medium">{label}</legend>
      {description && (
        <p className="mb-3 text-xs text-muted-foreground">{description}</p>
      )}
      {values.length > 0 ? (
        <div className="space-y-2">
          {values.map((value, index) => (
            <div key={index} className="flex min-w-0 items-center gap-2">
              <Label className="sr-only" htmlFor={`${id}-${index}`}>
                {label} {index + 1}
              </Label>
              <Input
                id={`${id}-${index}`}
                className="font-mono"
                value={value}
                aria-invalid={
                  requireNonEmpty && value === "" ? true : undefined
                }
                aria-describedby={
                  requireNonEmpty && value === "" ? `${id}-error` : undefined
                }
                onChange={(event) => {
                  const next = [...values];
                  next[index] = event.target.value;
                  onChange(next);
                }}
              />
              <Button
                type="button"
                variant="ghost"
                size="icon"
                aria-label={`Remove ${itemName} ${index + 1}`}
                onClick={() =>
                  onChange(values.filter((_, candidate) => candidate !== index))
                }
              >
                <Trash2 />
              </Button>
            </div>
          ))}
        </div>
      ) : (
        <p className="rounded-md bg-muted/50 px-3 py-2 text-sm text-muted-foreground">
          {emptyLabel}
        </p>
      )}
      {hasEmptyValue && (
        <p
          id={`${id}-error`}
          className="mt-2 text-xs text-destructive"
          role="alert"
        >
          Alias values cannot be empty.
        </p>
      )}
      <div className="mt-3 flex flex-wrap gap-2">
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => onChange([...values, ""])}
        >
          <Plus /> Add {itemName}
        </Button>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          disabled={values.length === 0}
          onClick={() => onChange([])}
        >
          <RotateCcw /> {resetLabel}
        </Button>
      </div>
    </fieldset>
  );
}

function DependencyEditor({
  index,
  name,
  dependency,
  serviceNames,
  onNameChange,
  onChange,
  onRemove,
}: {
  index: number;
  name: string;
  dependency: ServiceDependency;
  serviceNames: string[];
  onNameChange: (name: string) => void;
  onChange: (dependency: ServiceDependency) => void;
  onRemove: () => void;
}) {
  return (
    <div className="space-y-3 rounded-lg border border-border p-3">
      <div className="grid min-w-0 gap-3 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_auto] sm:items-end">
        <div className="flex min-w-0 flex-col gap-1.5">
          <Label htmlFor={`service-dependency-${index}`}>Service</Label>
          <Select
            searchable
            id={`service-dependency-${index}`}
            value={name}
            onValueChange={onNameChange}
            options={serviceNames.map((serviceName) => ({
              value: serviceName,
              label: serviceName,
            }))}
          />
        </div>
        <div className="flex min-w-0 flex-col gap-1.5">
          <Label htmlFor={`service-dependency-condition-${index}`}>
            Required state
          </Label>
          <Select
            id={`service-dependency-condition-${index}`}
            value={dependency.condition}
            onValueChange={(condition) =>
              onChange({
                ...dependency,
                condition: condition as ServiceDependencyCondition,
              })
            }
            options={[
              { value: "service_started", label: "Started" },
              { value: "service_healthy", label: "Healthy" },
              {
                value: "service_completed_successfully",
                label: "Completed successfully",
              },
            ]}
          />
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          aria-label={`Remove dependency on ${name}`}
          onClick={onRemove}
        >
          <Trash2 />
        </Button>
      </div>
      <fieldset>
        <legend className="mb-2 text-xs text-muted-foreground">
          Lifecycle phases (optional)
        </legend>
        <div className="flex flex-wrap gap-x-4 gap-y-2">
          {dependencyPhases.map((phase) => (
            <label
              key={phase.value}
              className="flex items-center gap-2 text-sm"
            >
              <Checkbox
                checked={dependency.phases.includes(phase.value)}
                onChange={(event) =>
                  onChange({
                    ...dependency,
                    phases: event.target.checked
                      ? [...dependency.phases, phase.value]
                      : dependency.phases.filter(
                          (candidate) => candidate !== phase.value,
                        ),
                  })
                }
                className="accent-primary"
              />
              {phase.label}
            </label>
          ))}
        </div>
      </fieldset>
    </div>
  );
}

function zoneLabel(env: Environment, zone: string) {
  return (
    env.zones.find(
      (candidate) => candidate.id === zone || candidate.name === zone,
    )?.name ?? zone
  );
}
