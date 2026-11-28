import { useId } from "react";
import { Plus, X } from "lucide-react";
import { Link } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { CodeEditor } from "@/components/ui/code-editor";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import type { Environment } from "@/lib/types";
import { newULID } from "@/lib/utils";
import type {
  FirstDeploymentDraft,
  FirstDeploymentVolume,
} from "./first-deployment-model";

const createVolume = "create:";

export function FirstDeploymentStorageMigrations({
  draft,
  volumes,
  environmentPath,
  disabled,
  onChange,
}: {
  draft: FirstDeploymentDraft;
  volumes: Environment["volumes"];
  environmentPath?: string;
  disabled: boolean;
  onChange: (update: Partial<FirstDeploymentDraft>) => void;
}) {
  const prefix = useId();
  const migration = draft.migration;
  const volumeOptions = [
    { value: createVolume, label: "Create a Volume" },
    ...volumes
      .filter((volume) => !volume.state || volume.state === "active")
      .map((volume) => ({ value: volume.key, label: volume.slug })),
  ];

  function updateVolume(id: string, change: Partial<FirstDeploymentVolume>) {
    onChange({
      volumes: draft.volumes.map((volume) =>
        volume.id === id ? { ...volume, ...change } : volume,
      ),
    });
  }

  if (!draft.newService) {
    return (
      <div className="space-y-2 rounded-lg border border-border p-4">
        <h3 className="text-sm font-medium">
          Persistent storage and migrations
        </h3>
        <p className="text-xs text-muted-foreground">
          This deployment uses the Service’s saved mounts and Scripts. Manage
          them in the Environment before reviewing an image change.
        </p>
        {environmentPath && (
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-sm">
            <Link
              className="text-primary hover:underline"
              to={`${environmentPath}?view=configuration&panel=volumes`}
            >
              Manage volumes
            </Link>
            <Link
              className="text-primary hover:underline"
              to={`${environmentPath}?view=configuration&panel=scripts`}
            >
              Manage scripts
            </Link>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-4 rounded-lg border border-border p-4">
      <section className="space-y-3" aria-labelledby={`${prefix}-volumes`}>
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h3 className="text-sm font-medium" id={`${prefix}-volumes`}>
            Persistent storage
          </h3>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() =>
              onChange({
                volumes: [
                  ...draft.volumes,
                  {
                    id: newULID(),
                    key: "",
                    target: "",
                    existing: false,
                    readOnly: false,
                  },
                ],
              })
            }
          >
            <Plus aria-hidden="true" /> Add volume mount
          </Button>
        </div>
        <p className="text-xs text-muted-foreground">
          Mount a new or existing Environment Volume at the container path where
          your application stores files across releases.
        </p>
        {draft.volumes.map((volume, index) => {
          const inputId = `${prefix}-volume-${volume.id}`;
          return (
            <div
              key={volume.id}
              className="grid gap-3 rounded-md border border-border p-3"
            >
              <div className="flex items-center justify-between gap-2">
                <p className="text-sm font-medium">Volume mount {index + 1}</p>
                <Button
                  type="button"
                  size="icon"
                  variant="ghost"
                  aria-label={`Remove mount ${index + 1}`}
                  onClick={() =>
                    onChange({
                      volumes: draft.volumes.filter(
                        (item) => item.id !== volume.id,
                      ),
                    })
                  }
                >
                  <X aria-hidden="true" />
                </Button>
              </div>
              <div className="grid gap-3 sm:grid-cols-2">
                <div className="space-y-2">
                  <Label htmlFor={inputId}>Volume</Label>
                  <Select
                    id={inputId}
                    searchable
                    value={volume.existing ? volume.key : createVolume}
                    options={volumeOptions}
                    onValueChange={(value) =>
                      updateVolume(volume.id, {
                        existing: value !== createVolume,
                        key: value === createVolume ? "" : value,
                      })
                    }
                  />
                </div>
                {!volume.existing && (
                  <label className="space-y-2 text-sm font-medium">
                    New volume name
                    <Input
                      value={volume.key}
                      onChange={(event) =>
                        updateVolume(volume.id, { key: event.target.value })
                      }
                      placeholder="app-data"
                      required
                    />
                  </label>
                )}
                <label className="space-y-2 text-sm font-medium">
                  Container path
                  <Input
                    value={volume.target}
                    onChange={(event) =>
                      updateVolume(volume.id, { target: event.target.value })
                    }
                    placeholder="/app/data"
                    required
                  />
                </label>
              </div>
              <label className="flex items-center gap-2 text-sm">
                <Checkbox
                  checked={volume.readOnly}
                  onChange={(event) =>
                    updateVolume(volume.id, { readOnly: event.target.checked })
                  }
                />
                Mount read-only
              </label>
            </div>
          );
        })}
      </section>

      <section
        className="space-y-3 border-t border-border pt-4"
        aria-labelledby={`${prefix}-migration`}
      >
        <div className="flex flex-wrap items-center justify-between gap-2">
          <h3 className="text-sm font-medium" id={`${prefix}-migration`}>
            Pre-deploy migration
          </h3>
          <Button
            type="button"
            variant="outline"
            size="sm"
            aria-expanded={Boolean(migration)}
            onClick={() =>
              onChange({
                migration: migration
                  ? undefined
                  : { slug: "migrate", script: "" },
              })
            }
          >
            {migration ? "Remove migration" : "Add migration script"}
          </Button>
        </div>
        {migration ? (
          <>
            <p
              className="text-xs text-muted-foreground"
              id={`${prefix}-migration-context`}
            >
              This Script runs once per selected Release using the candidate
              image and Service configuration before its containers start.
              Reference runtime variables for credentials and make the migration
              safe to rerun. Keep data changes compatible with the previous
              image. Image rollback cannot undo database or Volume writes.
            </p>
            <label className="block max-w-md space-y-2 text-sm font-medium">
              Script name
              <Input
                value={migration.slug}
                onChange={(event) =>
                  onChange({
                    migration: { ...migration, slug: event.target.value },
                  })
                }
                required
              />
            </label>
            <CodeEditor
              id={`${prefix}-migration-script`}
              label="Migration script"
              language="shell"
              scrollWithParent
              disabled={disabled}
              aria-describedby={`${prefix}-migration-context`}
              value={migration.script}
              onValueChange={(script) =>
                onChange({ migration: { ...migration, script } })
              }
            />
          </>
        ) : (
          <p className="text-xs text-muted-foreground">
            Add a Script if the application needs a schema or data migration
            before it starts.
          </p>
        )}
      </section>
      {(draft.volumes.some((volume) => !volume.existing) || migration) && (
        <p className="text-xs text-muted-foreground">
          New Volumes and Scripts are saved by an Environment Blueprint Apply.
          Review the proposed operations before deploying.
        </p>
      )}
    </div>
  );
}
