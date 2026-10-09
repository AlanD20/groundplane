import type { Environment } from "@/lib/types";
import type { ScriptExecution } from "@/features/script/types";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { ScriptVolumeGrants } from "./script-volume-grants";
import { ScriptEntryGrants } from "./script-entry-grants";

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
        <Label htmlFor="script-execution-mode">Execution context</Label>
        <Select
          id="script-execution-mode"
          value={execution.mode}
          options={[
            { value: "inherited", label: "Inherited Service context" },
            { value: "explicit", label: "Explicit setup context" },
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
            Isolated image with no network; only the grants below are available.
            Requires a serving Release.
          </p>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="script-execution-image">
              Pinned execution image
            </Label>
            <Input
              id="script-execution-image"
              value={execution.image}
              spellCheck={false}
              className="font-mono text-xs"
              placeholder="registry.example/setup@sha256:…"
              onChange={(event) =>
                onChange({ ...execution, image: event.target.value })
              }
            />
            <p className="text-xs text-muted-foreground">
              Use an image digest already on this host.
            </p>
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="script-execution-user">
              Numeric execution user
            </Label>
            <Input
              id="script-execution-user"
              value={execution.user}
              spellCheck={false}
              placeholder="0:0"
              className="font-mono text-xs"
              onChange={(event) =>
                onChange({ ...execution, user: event.target.value })
              }
            />
            <p className="text-xs text-muted-foreground">
              User and group: uid:gid.
            </p>
          </div>
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
