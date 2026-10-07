"use client";

import { SecretReferencePicker } from "@/components/common/secret-reference-picker";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import type {
  ConnectorCredential,
  ConnectorCredentialInput,
  ReusableSecret,
} from "@/lib/types";

export type CredentialDraft = {
  kind: "keep" | ConnectorCredentialInput["kind"];
  value: string;
};

export function ConnectorCredentialField({
  id,
  label,
  draft,
  existing,
  onChange,
  secretOptions,
}: {
  id: string;
  label: string;
  draft: CredentialDraft;
  existing?: ConnectorCredential;
  onChange: (draft: CredentialDraft) => void;
  secretOptions: ReusableSecret[];
}) {
  const keepLabel = existing
    ? existing.kind === "ref"
      ? `Keep existing secret ref : ${existing.name}`
      : "Keep existing direct encrypted value"
    : "Keep existing credential";
  return (
    <fieldset className="flex min-w-0 flex-col gap-2 rounded-lg border border-border bg-surface p-3">
      <legend className="px-1 text-xs font-medium">{label}</legend>
      <Label htmlFor={`${id}-kind`}>Source</Label>
      <Select
        id={`${id}-kind`}
        value={draft.kind}
        onValueChange={(kind) =>
          onChange({ kind: kind as CredentialDraft["kind"], value: "" })
        }
        options={[
          ...(existing ? [{ value: "keep", label: keepLabel }] : []),
          { value: "ref", label: "Reusable Secret" },
          { value: "value", label: "Enter encrypted value" },
        ]}
      />
      {draft.kind !== "keep" && (
        <>
          <Label htmlFor={id}>
            {draft.kind === "ref" ? "Secret key name" : "Credential value"}
          </Label>
          {draft.kind === "ref" ? (
            <SecretReferencePicker
              id={id}
              value={draft.value}
              onChange={(value) => onChange({ ...draft, value })}
              options={[
                ...new Map(
                  secretOptions.map((secret) => [
                    secret.key,
                    { value: secret.key, label: secret.key },
                  ]),
                ).values(),
              ]}
            />
          ) : (
            <Input
              id={id}
              type="password"
              value={draft.value}
              autoComplete="off"
              onChange={(event) =>
                onChange({ ...draft, value: event.target.value })
              }
              placeholder="Stored encrypted at rest"
            />
          )}
        </>
      )}
      <p className="text-[11px] text-muted-foreground">
        {draft.kind === "keep"
          ? "The existing source remains unchanged. Its value is not read."
          : draft.kind === "ref"
            ? "The connector stores only the secret name."
            : "The Controller encrypts this value and never returns it."}
      </p>
    </fieldset>
  );
}
