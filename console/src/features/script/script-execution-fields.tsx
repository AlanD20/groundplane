import type { Environment } from "@/lib/types";
import type { ScriptExecution } from "@/features/script/types";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { ScriptVolumeGrants } from "./script-volume-grants";
import { ScriptEntryGrants } from "./script-entry-grants";
import { ImagePicker } from "@/features/image-delivery/image-picker";
import { HelpHint } from "@/components/common/resource-panel";

export function ScriptExecutionFields({
  env,
  service,
  execution,
  onChange,
}: {
  env: Environment;
  service: string;
  execution: ScriptExecution;
  onChange: (execution: ScriptExecution) => void;
}) {
  return (
    <section
      className="flex min-w-0 flex-col gap-4 border-t border-border pt-4"
      aria-label="Script execution context"
    >
      <div className="flex flex-col gap-1.5">
        <Label htmlFor="script-execution-mode">Run using</Label>
        <Select
          id="script-execution-mode"
          value={execution.mode}
          options={[
            { value: "inherited", label: "Service configuration" },
            { value: "explicit", label: "Separate container" },
          ]}
          onValueChange={(mode) => {
            if (mode === execution.mode) return;
            onChange(
              mode === "inherited"
                ? { mode: "inherited" }
                : {
                    mode: "explicit",
                    image: "",
                    user: "",
                    volumes: [],
                    entryIds: [],
                  },
            );
          }}
        />
      </div>
      {execution.mode === "inherited" ? (
        <p className="text-xs text-muted-foreground">
          Uses the Service’s execution context.
        </p>
      ) : (
        <>
          <p className="text-xs text-muted-foreground">
            No network access. Only the selected Volumes, variables and files
            are available.
          </p>
          <div className="flex flex-col gap-1.5">
            <div className="flex items-center gap-1.5">
              <Label htmlFor="script-execution-image">Container image</Label>
              <HelpHint label="About Script images">
                Scripts use an exact image digest already on this host, so the
                image cannot change between runs.
              </HelpHint>
            </div>
            <ImagePicker
              id="script-execution-image"
              value={execution.image}
              placeholder="registry.example/setup@sha256:…"
              referencesForImage={(image) => image.digests}
              onChange={(image) => onChange({ ...execution, image })}
            />
          </div>
          <fieldset className="space-y-2">
            <legend className="flex items-center gap-1.5 text-sm font-medium">
              Run as
              <HelpHint label="About container permissions">
                Linux user and group IDs control file access inside this
                container. 0 is root; use IDs that have the required access to
                mounted files.
              </HelpHint>
            </legend>
            <div className="grid max-w-sm grid-cols-2 gap-3">
              {(["User ID (UID)", "Group ID (GID)"] as const).map(
                (label, index) => (
                  <div key={label} className="space-y-1.5">
                    <Label htmlFor={`script-execution-user-${index}`}>
                      {label}
                    </Label>
                    <Input
                      id={`script-execution-user-${index}`}
                      inputMode="numeric"
                      value={execution.user.split(":")[index] ?? ""}
                      placeholder="1000"
                      onChange={(event) => {
                        const parts = execution.user.split(":");
                        parts[index] = event.target.value;
                        onChange({
                          ...execution,
                          user: `${parts[0] ?? ""}:${parts[1] ?? ""}`,
                        });
                      }}
                    />
                  </div>
                ),
              )}
            </div>
          </fieldset>
          <ScriptVolumeGrants
            volumes={env.volumes}
            grants={execution.volumes}
            onChange={(volumes) => onChange({ ...execution, volumes })}
          />
          <ScriptEntryGrants
            entries={env.entries}
            service={service}
            selected={execution.entryIds}
            onChange={(entryIds) => onChange({ ...execution, entryIds })}
          />
        </>
      )}
    </section>
  );
}
