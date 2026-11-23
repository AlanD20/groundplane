import { BulkConfigurationMenu, type BulkConfigurationKind } from "@/components/common/bulk-configuration-menu";
import {
  ConfigurationFields,
  newConfigurationDraft,
} from "@/features/secrets/configuration-fields";
import { BulkSecretEditor } from "@/features/secrets/bulk-secret-editor";
import { CreationButton } from "@/components/common/creation-button";
("use client");

import { PageHeader } from "@/components/common/page-header";
import { HelpHint, ResourcePanel } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import {
  ResourceFormFooter as DialogFooter,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceForm,
} from "@/components/common/resource-form";
import { ReusableSecretOwnerList } from "@/features/secrets/reusable-secret-owner-list";
import { useStore } from "@/lib/store";
import { KeyRound, Plus } from "lucide-react";
import { useState } from "react";
export default function PlatformSecretsPage() {
  const store = useStore();
  const [open, setOpen] = useState(false);
  const [bulkKind, setBulkKind] = useState<BulkConfigurationKind | null>(null);
  const [draft, setDraft] = useState(newConfigurationDraft);
  const { kind, key, value, path, sensitive, encryption } = draft;
  const [creating, setCreating] = useState(false);
  const [mutationError, setMutationError] = useState<string | null>(null);

  function clearDraft() {
    setDraft(newConfigurationDraft());
  }

  const reusableSecrets = store.reusableSecrets.filter(
    (secret) => secret.scope === "platform",
  );
  const error = mutationError ?? store.secretError;
  async function create() {
    setCreating(true);
    setMutationError(null);
    try {
      await store.createReusableSecret({
        platform: true,
        key: key.trim(),
        kind,
        path: kind === "file" ? path.trim() : undefined,
        value,
        sensitive,
        encryption,
      });
      setOpen(false);
      clearDraft();
    } catch (cause) {
      setDraft((current) => ({ ...current, value: "" }));
      setMutationError(
        cause instanceof Error ? cause.message : "Unable to create Secret",
      );
    } finally {
      setCreating(false);
    }
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title="Configuration"
        eyebrow="Platform"
        icon={<KeyRound />}
        actions={
          <div className="flex flex-wrap gap-2">
            <BulkConfigurationMenu disabled={open || bulkKind !== null} onSelect={setBulkKind} />
            <CreationButton
              active={open}
              disabled={bulkKind !== null}
              onOpenChange={setOpen}
            >
              <Plus className="size-4" /> Add configuration
            </CreationButton>
          </div>
        }
      />
      {bulkKind && <BulkSecretEditor
        key={bulkKind}
        kind={bulkKind}
        open
        onOpenChange={(next) => { if (!next) setBulkKind(null); }}
        secrets={store.reusableSecrets.filter(
          (secret) => secret.scope === "platform",
        )}
      />}
      <ResourceForm
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) clearDraft();
        }}
      >
        <DialogHeader>
          <DialogTitle>New platform variable or file</DialogTitle>
        </DialogHeader>
        <ConfigurationFields draft={draft} onChange={setDraft} />
        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => {
              setOpen(false);
              clearDraft();
            }}
            disabled={creating}
          >
            Cancel
          </Button>
          <Button
            disabled={
              creating || !key.trim() || (kind === "file" && !path.trim())
            }
            onClick={() => void create()}
          >
            {creating ? "Saving…" : "Save configuration"}
          </Button>
        </DialogFooter>
      </ResourceForm>

      {error && <p className="text-sm text-destructive">{error}</p>}

      <ResourcePanel
        title="Configuration values"
        actions={
          <HelpHint>
            Platform-owned metadata. Plaintext is returned only by the explicit
            reveal action and is never listed with these references.
          </HelpHint>
        }
      >
        <ReusableSecretOwnerList
          secrets={reusableSecrets}
          loading={store.reusableSecretsLoading}
          emptyTitle="No platform configuration yet"
          scopeLabel="Platform"
          editable
          onRemove={(secret, after) =>
            store.removeReusableSecret(secret.id, after)
          }
          onRefresh={() => store.refreshReusableSecrets()}
          onReveal={(secret) => store.revealReusableSecret(secret.id)}
        />
      </ResourcePanel>
    </div>
  );
}
