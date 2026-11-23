import {
  BulkConfigurationMenu,
  type BulkConfigurationKind,
} from "@/components/common/bulk-configuration-menu";
import {
  ConfigurationFields,
  newConfigurationDraft,
} from "@/features/secrets/configuration-fields";
import { BulkSecretEditor } from "@/features/secrets/bulk-secret-editor";
import { CreationButton } from "@/components/common/creation-button";
("use client");

import { ResourceLookupState } from "@/components/common/resource-lookup-state";
import { MetaPill } from "@/components/common/meta-pill";
import { PageHeader } from "@/components/common/page-header";
import { ResourcePanel } from "@/components/common/resource-panel";
import { Button } from "@/components/ui/button";
import {
  ResourceFormFooter as DialogFooter,
  ResourceFormHeader as DialogHeader,
  ResourceFormTitle as DialogTitle,
  ResourceForm,
} from "@/components/common/resource-form";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { ArrowLeft, KeyRound, Plus } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";
import { ReusableSecretOwnerList } from "./reusable-secret-owner-list";
export default function ProjectSecretsPage() {
  const params = useRequiredParams("tenant", "project");
  const store = useStore();
  const tenant = store.getTenant(params.tenant);
  const project = store.getProject(params.tenant, params.project);
  const [open, setOpen] = useState(false);
  const [bulkKind, setBulkKind] = useState<BulkConfigurationKind | null>(null);
  const [draft, setDraft] = useState(newConfigurationDraft);
  const { kind, key, value, path, sensitive, encryption } = draft;
  const [creating, setCreating] = useState(false);
  const [mutationError, setMutationError] = useState<string | null>(null);

  function clearDraft() {
    setDraft(newConfigurationDraft());
  }
  if (!tenant || !project || project.tenantId !== tenant.id) {
    return (
      <ResourceLookupState
        icon={<KeyRound />}
        resource="Project"
        loading={store.tenantsLoading || store.projectsLoading}
        error={store.tenantError ?? store.projectError}
        description={`${params.tenant}/${params.project} does not exist.`}
        action={
          <Link to={`/t/${params.tenant}`}>
            <Button variant="outline">
              <ArrowLeft className="size-4" /> Back to tenant
            </Button>
          </Link>
        }
      />
    );
  }

  const projectId = project.id;
  const scoped = store.reusableSecrets.filter(
    (secret) => secret.scope === "project" && secret.projectId === projectId,
  );
  const fallbacks = store.reusableSecrets.filter(
    (secret) => secret.scope === "platform",
  );
  const error = mutationError ?? store.secretError;
  async function create() {
    setCreating(true);
    setMutationError(null);
    try {
      await store.createReusableSecret({
        projectId,
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
        eyebrow={`Project · ${project.name}`}
        icon={<KeyRound />}
        meta={
          <MetaPill icon={<KeyRound />}>
            {scoped.length} configuration{" "}
            {scoped.length === 1 ? "value" : "values"}
          </MetaPill>
        }
        actions={
          <div className="flex flex-wrap gap-2">
            <BulkConfigurationMenu
              disabled={open || bulkKind !== null}
              onSelect={setBulkKind}
            />
            <CreationButton
              active={open}
              disabled={bulkKind !== null}
              onClick={() => setOpen(true)}
            >
              <Plus className="size-4" /> Add configuration
            </CreationButton>
          </div>
        }
      />
      {bulkKind && (
        <BulkSecretEditor
          key={bulkKind}
          kind={bulkKind}
          open
          onOpenChange={(next) => {
            if (!next) setBulkKind(null);
          }}
          projectId={projectId}
          secrets={scoped}
        />
      )}
      <ResourceForm
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) clearDraft();
        }}
      >
        <DialogHeader>
          <DialogTitle>Add configuration · {project.name}</DialogTitle>
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

      <ResourcePanel title="Configuration values">
        <p className="text-sm text-muted-foreground">
          Owned by {project.name}. A matching Project key takes precedence over
          the Platform fallback.
        </p>
        <ReusableSecretOwnerList
          secrets={scoped}
          loading={store.reusableSecretsLoading}
          emptyTitle="No project configuration"
          emptyDescription="Resolution falls back to matching Platform values."
          scopeLabel="Project"
          editable
          onRemove={(secret, after) =>
            store.removeReusableSecret(secret.id, after)
          }
          onRefresh={() => store.refreshReusableSecrets()}
          onReveal={(secret) => store.revealReusableSecret(secret.id)}
        />
      </ResourcePanel>

      <ResourcePanel title="Platform fallback configuration">
        <p className="text-sm text-muted-foreground">
          Owned by the Platform and used only when this Project has no matching
          key. Manage these shared resources in Platform Configuration.
        </p>
        <ReusableSecretOwnerList
          secrets={fallbacks}
          loading={store.reusableSecretsLoading}
          emptyTitle="No platform fallback configuration"
          scopeLabel="Platform fallback"
          onReveal={(secret) => store.revealReusableSecret(secret.id)}
        />
      </ResourcePanel>
    </div>
  );
}
