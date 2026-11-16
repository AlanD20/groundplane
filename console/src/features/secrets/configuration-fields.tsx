import { useId } from "react";
import { ConfigurationTypeOptions } from "@/components/common/configuration-type-options";
import { Checkbox } from "@/components/ui/checkbox";
import { CodeEditor } from "@/components/ui/code-editor";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export type ConfigurationDraft = {
  kind: "env" | "file";
  key: string;
  value: string;
  path: string;
  sensitive: boolean;
  encryption: "age" | "none";
};

export function newConfigurationDraft(): ConfigurationDraft {
  return {
    kind: "env",
    key: "",
    value: "",
    path: "",
    sensitive: true,
    encryption: "age",
  };
}

/** Shared creation controls for reusable Project and Platform configuration. */
export function ConfigurationFields({
  draft,
  onChange,
}: {
  draft: ConfigurationDraft;
  onChange: (draft: ConfigurationDraft) => void;
}) {
  const id = useId();
  const type =
    draft.kind === "file" ? "file" : draft.sensitive ? "secret" : "variable";
  const update = (change: Partial<ConfigurationDraft>) =>
    onChange({ ...draft, ...change });
  return (
    <div className="grid max-w-5xl items-start gap-4 md:grid-cols-2">
      <div className="md:col-span-2">
        <ConfigurationTypeOptions
          value={type}
          onChange={(candidate) =>
            update(
              candidate === "file"
                ? { kind: "file" }
                : {
                    kind: "env",
                    sensitive: candidate === "secret",
                    encryption: candidate === "secret" ? "age" : "none",
                  },
            )
          }
        />
      </div>
      <div className="space-y-1.5">
        <Label htmlFor={`${id}-key`}>
          {draft.kind === "file" ? "Name" : "Key"}
        </Label>
        <Input
          id={`${id}-key`}
          autoFocus
          value={draft.key}
          onChange={(event) => update({ key: event.target.value })}
          placeholder={
            draft.kind === "file"
              ? "application-config"
              : type === "secret"
                ? "API_TOKEN"
                : "APP_ENV"
          }
        />
      </div>
      {draft.kind === "file" ? (
        <div className="space-y-1.5">
          <Label htmlFor={`${id}-path`}>File path</Label>
          <Input
            id={`${id}-path`}
            value={draft.path}
            placeholder="config/application.yml"
            onChange={(event) => update({ path: event.target.value })}
          />
          <p className="text-xs text-muted-foreground">
            Relative to the Environment volume.
          </p>
        </div>
      ) : (
        <div className="space-y-1.5">
          <Label htmlFor={`${id}-value`}>Value</Label>
          <Input
            id={`${id}-value`}
            value={draft.value}
            type={draft.sensitive ? "password" : "text"}
            autoComplete="off"
            placeholder={draft.sensitive ? "Enter secret value" : "Enter value"}
            onChange={(event) => update({ value: event.target.value })}
          />
        </div>
      )}
      {draft.kind === "file" && (
        <div className="space-y-1.5 md:col-span-2">
          <CodeEditor
            id={`${id}-content`}
            label="Contents"
            value={draft.value}
            onValueChange={(value) => update({ value })}
          />
          <label className="flex items-center gap-2 text-sm">
            <Checkbox
              checked={draft.sensitive}
              onChange={(event) => update({ sensitive: event.target.checked })}
            />
            Treat file contents as secret
          </label>
        </div>
      )}
      <div className="space-y-2 border-t border-border pt-4 md:col-span-2">
        <label className="flex items-center gap-2 text-sm font-medium">
          <Checkbox
            checked={draft.encryption === "age"}
            onChange={(event) =>
              update({ encryption: event.target.checked ? "age" : "none" })
            }
          />
          Encrypt stored value
        </label>
        <p className="text-xs text-muted-foreground">
          {draft.encryption === "age"
            ? "Encrypted at rest in the Controller."
            : "Stored without encryption in the Controller."}{" "}
          {draft.sensitive
            ? "Hidden until explicitly revealed, regardless of encryption."
            : "Not classified as secret."}
        </p>
      </div>
    </div>
  );
}
