"use client";

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
  const listID = `${id}-secrets`;
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
          { value: "ref", label: "Secret-store reference" },
          { value: "value", label: "Direct encrypted value" },
        ]}
      />
      {draft.kind !== "keep" && (
        <>
          <Label htmlFor={id}>
            {draft.kind === "ref" ? "Secret key name" : "Credential value"}
          </Label>
          <Input
            id={id}
            type={draft.kind === "value" ? "password" : "text"}
            list={draft.kind === "ref" ? listID : undefined}
            value={draft.value}
            onChange={(event) =>
              onChange({ ...draft, value: event.target.value })
            }
            placeholder={
              draft.kind === "ref"
                ? "R2_ACCESS_KEY_ID"
                : "Stored encrypted at rest"
            }
            autoComplete="off"
          />
          {draft.kind === "ref" && (
            <datalist id={listID}>
              {secretOptions.map((secret) => (
                <option key={secret.id} value={secret.key} />
              ))}
            </datalist>
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
