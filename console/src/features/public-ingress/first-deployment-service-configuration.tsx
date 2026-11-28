import { useId } from "react";
import { Plus, Trash2 } from "lucide-react";
import { SecretReferencePicker } from "@/components/common/secret-reference-picker";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, type SelectOption } from "@/components/ui/select";
import type { Environment } from "@/lib/types";
import { newULID } from "@/lib/utils";
import type {
  FirstDeploymentDraft,
  FirstDeploymentEntry,
} from "./first-deployment-model";

const create = "__create__";

export function FirstDeploymentServiceConfiguration({
  draft,
  environment,
  secretOptions = [],
  onChange,
}: {
  draft: FirstDeploymentDraft;
  environment: Pick<Environment, "networkPool"> & {
    zones: Pick<Environment["zones"][number], "name" | "subnet">[];
  };
  secretOptions?: SelectOption[];
  onChange: (update: Partial<FirstDeploymentDraft>) => void;
}) {
  const prefix = useId();

  function updateEntry(id: string, change: Partial<FirstDeploymentEntry>) {
    onChange({
      entries: draft.entries.map((entry) =>
        entry.id === id ? { ...entry, ...change } : entry,
      ),
    });
  }

  return (
    <>
      {draft.newService && (
        <div className="space-y-3 rounded-lg border border-border p-4">
          <p className="text-sm font-medium">Application networking</p>
          <div className="grid gap-3 sm:grid-cols-2">
            <label className="space-y-2 text-sm font-medium">
              Container port
              <Input
                type="number"
                min={1}
                max={65535}
                value={draft.port}
                onChange={(event) => onChange({ port: event.target.value })}
                required
              />
            </label>
            <label className="space-y-2 text-sm font-medium">
              HTTP readiness path
              <Input
                value={draft.healthPath}
                onChange={(event) =>
                  onChange({ healthPath: event.target.value })
                }
                required
              />
            </label>
          </div>
          <p className="text-xs text-muted-foreground">
            The readiness check runs curl inside the container. Choose an image
            that includes curl and serves this path.
          </p>
          <div className="space-y-2">
            <Label htmlFor={`${prefix}-zone`}>Application Zone</Label>
            <Select
              id={`${prefix}-zone`}
              searchable
              value={draft.newZone ? create : draft.zone}
              options={[
                { value: create, label: "Create a private Zone…" },
                ...environment.zones.map((zone) => ({
                  value: zone.name,
                  label: `${zone.name} · ${zone.subnet}`,
                })),
              ]}
              onValueChange={(value) =>
                onChange({
                  zone: value === create ? "application" : value,
                  newZone: value === create,
                })
              }
            />
          </div>
          {draft.newZone && (
            <div className="grid gap-3 sm:grid-cols-2">
              <label className="space-y-2 text-sm font-medium">
                Zone name
                <Input
                  value={draft.zone}
                  onChange={(event) => onChange({ zone: event.target.value })}
                  required
                />
              </label>
              <label className="space-y-2 text-sm font-medium">
                Zone subnet
                <Input
                  value={draft.subnet}
                  onChange={(event) => onChange({ subnet: event.target.value })}
                  required
                />
              </label>
            </div>
          )}
          <p className="text-xs text-muted-foreground">
            Use an unused subnet inside {environment.networkPool}. This setup
            creates no public Route.
          </p>
        </div>
      )}
      <div className="space-y-3">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <span className="text-sm font-medium">Service configuration</span>
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() =>
              onChange({
                entries: [
                  ...draft.entries,
                  {
                    id: newULID(),
                    key: "",
                    source: "literal",
                    value: "",
                  },
                ],
              })
            }
          >
            <Plus aria-hidden className="size-4" /> Add variable
          </Button>
        </div>
        {draft.entries.map((entry) => (
          <div
            key={entry.id}
            className="grid gap-2 rounded-lg border border-border p-3 sm:grid-cols-[minmax(0,1fr)_minmax(0,1fr)_minmax(0,1fr)_auto]"
          >
            <Input
              aria-label="Variable name"
              value={entry.key}
              onChange={(event) =>
                updateEntry(entry.id, { key: event.target.value })
              }
              required
            />
            <Select
              aria-label="Variable source"
              value={entry.source}
              options={[
                { value: "literal", label: "Plain variable" },
                {
                  value: "secret_ref",
                  label: "Existing Secret reference",
                },
              ]}
              onValueChange={(value) => {
                if (value === "literal" || value === "secret_ref")
                  updateEntry(entry.id, { source: value, value: "" });
              }}
            />
            {entry.source === "secret_ref" ? (
              <SecretReferencePicker
                id={`${prefix}-${entry.id}`}
                value={entry.value}
                options={secretOptions}
                onChange={(value) => updateEntry(entry.id, { value })}
              />
            ) : (
              <Input
                aria-label="Variable value"
                value={entry.value}
                placeholder="Value"
                onChange={(event) =>
                  updateEntry(entry.id, { value: event.target.value })
                }
              />
            )}
            <Button
              type="button"
              variant="ghost"
              size="icon"
              aria-label="Remove variable"
              onClick={() =>
                onChange({
                  entries: draft.entries.filter(({ id }) => id !== entry.id),
                })
              }
            >
              <Trash2 aria-hidden className="size-4" />
            </Button>
          </div>
        ))}
        <p className="text-xs text-muted-foreground">
          Reference an existing Project or Platform Secret to expose it to this
          Service.
        </p>
      </div>
    </>
  );
}
