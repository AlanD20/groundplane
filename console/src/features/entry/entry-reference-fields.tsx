import { useEffect, useState } from "react";
import { SecretReferencePicker } from "@/components/common/secret-reference-picker";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";
import { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";

type ConnectionSource = {
  attachId: string;
  grantAttachId: string;
  key: string;
};
export function EntryReferenceFields({
  env,
  projectId,
  sourceKind,
  kind,
  secretRef,
  connectionSource,
  onSecretChange,
  onConnectionChange,
}: {
  env: Environment;
  projectId?: string;
  sourceKind: "secret_ref" | "fact";
  kind: "env" | "file";
  secretRef: string;
  connectionSource: ConnectionSource;
  onSecretChange: (value: string) => void;
  onConnectionChange: (value: ConnectionSource) => void;
}) {
  const store = useStore();
  const [loadError, setLoadError] = useState<string>();
  const {
    attachId: factAttach,
    grantAttachId: factGrantAttach,
    key: factKey,
  } = connectionSource;
  const secretOptions = store.reusableSecrets.filter(
    (secret) =>
      secret.kind === kind &&
      (secret.scope === "platform" || secret.projectId === projectId),
  );
  const connection = env.attaches.find((attach) => attach.id === factAttach);
  const factSet = connection?.factSets.find(
    (set) => (set.grantAttachId ?? "") === factGrantAttach,
  );
  useEffect(() => {
    if (sourceKind !== "secret_ref") return;
    let current = true;
    void store.refreshReusableSecrets().catch(() => {
      if (current)
        setLoadError(
          "Unable to load Secret suggestions. You can still enter a known reference.",
        );
    });
    return () => {
      current = false;
    };
  }, [sourceKind, store.refreshReusableSecrets]);

  return (
    <>
      {sourceKind === "secret_ref" && (
        <div className="flex flex-col gap-1.5">
          <Label htmlFor="entry-secret-ref">
            Project or Platform configuration
          </Label>
          <SecretReferencePicker
            id="entry-secret-ref"
            noun="configuration"
            value={secretRef}
            onChange={onSecretChange}
            options={secretOptions.map((secret) => ({
              value: secret.id,
              label: `${secret.key} · ${secret.scope === "platform" ? "Platform" : "Project"} · ${secret.sensitive ? "Secret" : secret.kind === "file" ? "File" : "Variable"}`,
            }))}
          />
          <p className="text-xs text-muted-foreground">
            Choose an owned value, or enter a key for Project-before-Platform
            lookup.
          </p>
        </div>
      )}
      {sourceKind === "fact" && (
        <div className="grid gap-3">
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="entry-fact-attach">Backing connection</Label>
            <Select
              searchable
              id="entry-fact-attach"
              value={factAttach}
              placeholder="Choose a connection"
              options={[
                ...env.attaches.map((attach) => ({
                  value: attach.id,
                  label: `${attach.name} · ${attach.service} · ${store.getBackingProject(attach.projectId)?.name ?? "Backing Service"}`,
                })),
                ...(factAttach && !connection
                  ? [
                      {
                        value: factAttach,
                        label: `Unavailable connection · ${factAttach}`,
                      },
                    ]
                  : []),
              ]}
              onValueChange={(value) => {
                onConnectionChange({
                  attachId: value,
                  grantAttachId: "",
                  key: "",
                });
              }}
            />
            {!env.attaches.length && (
              <p className="text-xs text-muted-foreground">
                Connect a backing service first, then choose one of its
                published values.
              </p>
            )}
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="entry-fact-grant">Credential scope</Label>
            <Select
              searchable
              id="entry-fact-grant"
              value={factGrantAttach || "own"}
              disabled={!connection}
              options={[
                { value: "own", label: "This connection" },
                ...(connection?.factSets
                  .filter((set) => set.grantAttachId)
                  .map((set) => ({
                    value: set.grantAttachId!,
                    label: `Granted database · ${env.attaches.find((attach) => attach.id === set.grantAttachId)?.name ?? set.grantAttachId}`,
                  })) ?? []),
                ...(factGrantAttach &&
                !connection?.factSets.some(
                  (set) => set.grantAttachId === factGrantAttach,
                )
                  ? [
                      {
                        value: factGrantAttach,
                        label: `Unavailable grant · ${factGrantAttach}`,
                      },
                    ]
                  : []),
              ]}
              onValueChange={(value) => {
                onConnectionChange({
                  attachId: factAttach,
                  grantAttachId: value === "own" ? "" : value,
                  key: "",
                });
              }}
            />
          </div>
          <div className="flex flex-col gap-1.5">
            <Label htmlFor="entry-fact-key">Connection value</Label>
            <Select
              searchable
              id="entry-fact-key"
              value={factKey}
              disabled={!connection}
              placeholder="Choose a published value"
              options={[
                ...(factSet?.facts.map((fact) => ({
                  value: fact.key,
                  label: `${fact.key}${fact.secret ? " · sensitive" : ""}`,
                })) ?? []),
                ...(factKey &&
                !factSet?.facts.some((fact) => fact.key === factKey)
                  ? [
                      {
                        value: factKey,
                        label: `Unavailable value · ${factKey}`,
                      },
                    ]
                  : []),
              ]}
              onValueChange={(value) =>
                onConnectionChange({
                  attachId: factAttach,
                  grantAttachId: factGrantAttach,
                  key: value,
                })
              }
            />
          </div>
        </div>
      )}

      {loadError && sourceKind === "secret_ref" && (
        <p role="alert" className="text-sm text-destructive">
          {loadError}
        </p>
      )}
    </>
  );
}
