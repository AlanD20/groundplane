import { Plus, Trash2 } from "lucide-react";
import type { Environment } from "@/lib/types";
import type { ScriptVolumeGrant } from "@/features/script/types";
import { maximumScriptVolumes } from "@/features/script/execution";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";

export function ScriptVolumeGrants({
  volumes,
  grants,
  onChange,
}: {
  volumes: Environment["volumes"];
  grants: ScriptVolumeGrant[];
  onChange: (grants: ScriptVolumeGrant[]) => void;
}) {
  const available = volumes.filter(
    (volume) => !grants.some((grant) => grant.volumeId === volume.id),
  );
  function update(index: number, patch: Partial<ScriptVolumeGrant>) {
    onChange(
      grants.map((grant, position) =>
        position === index ? { ...grant, ...patch } : grant,
      ),
    );
  }
  return (
    <fieldset className="flex min-w-0 flex-col gap-3">
      <legend className="mb-2 text-sm font-medium">
        Volume grants ({grants.length}/{maximumScriptVolumes})
      </legend>
      {grants.map((grant, index) => {
        const current = volumes.find((volume) => volume.id === grant.volumeId);
        const options = volumes
          .filter(
            (volume) =>
              volume.id === grant.volumeId ||
              !grants.some((selected) => selected.volumeId === volume.id),
          )
          .map((volume) => ({ value: volume.id, label: volume.slug }));
        if (!current)
          options.push({
            value: grant.volumeId,
            label: `${grant.volumeId} (unavailable)`,
          });
        return (
          <div
            key={index}
            className="grid min-w-0 gap-3 rounded-lg border border-border p-3 sm:grid-cols-2"
          >
            <div className="flex min-w-0 flex-col gap-1.5">
              <Label htmlFor={`script-volume-${index}`}>
                Volume {index + 1}
              </Label>
              <Select
                id={`script-volume-${index}`}
                value={grant.volumeId}
                options={options}
                onValueChange={(volumeId) => update(index, { volumeId })}
              />
            </div>
            <div className="flex min-w-0 flex-col gap-1.5">
              <Label htmlFor={`script-target-${index}`}>
                Mount target {index + 1}
              </Label>
              <Input
                id={`script-target-${index}`}
                value={grant.target}
                placeholder="/etc/tls"
                spellCheck={false}
                onChange={(event) =>
                  update(index, { target: event.target.value })
                }
                className="font-mono text-xs"
              />
            </div>
            <div className="flex items-center gap-2">
              <Switch
                id={`script-readonly-${index}`}
                checked={grant.readOnly}
                onCheckedChange={(readOnly) => update(index, { readOnly })}
              />
              <Label htmlFor={`script-readonly-${index}`}>
                Read-only {index + 1}
              </Label>
              <span className="text-xs text-muted-foreground">
                {grant.readOnly ? "Read-only" : "Read-write"}
              </span>
            </div>
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="justify-self-end"
              aria-label={`Remove Volume grant ${index + 1}`}
              onClick={() =>
                onChange(grants.filter((_, position) => position !== index))
              }
            >
              <Trash2 className="size-3.5" /> Remove
            </Button>
            {!current && (
              <p className="text-xs text-muted-foreground sm:col-span-2">
                This saved Volume is unavailable. Replace or remove its grant
                before running the Script.
              </p>
            )}
          </div>
        );
      })}
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="self-start"
        disabled={
          available.length === 0 || grants.length >= maximumScriptVolumes
        }
        onClick={() =>
          available[0] &&
          onChange([
            ...grants,
            { volumeId: available[0].id, target: "", readOnly: true },
          ])
        }
      >
        <Plus className="size-3.5" /> Volume grant
      </Button>
      <p className="text-xs text-muted-foreground">
        Only these managed Volumes are mounted. No Service mounts are inherited;
        empty Volumes are not populated from the image.
      </p>
    </fieldset>
  );
}
