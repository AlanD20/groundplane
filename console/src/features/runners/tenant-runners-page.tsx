"use client";

import { Select } from "@/components/ui/select";
import { TaskLink } from "@/components/common/task-link";

import { useEffect, useMemo, useState } from "react";
import { Cpu, GitBranch, Plus } from "lucide-react";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import { PageHeader } from "@/components/common/page-header";
import { EmptyState } from "@/components/common/empty-state";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import type { Runner } from "@/lib/types";
import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { RunnerInventory } from "./runner-inventory";

export default function TenantRunnersPage() {
  const params = useRequiredParams("tenant");
  const store = useStore();
  const tenant = store.getTenant(params.tenant);
  const tenantId = tenant?.id ?? "";
  const projects = useMemo(
    () =>
      store.tenantProjects.filter((project) => project.tenantId === tenantId),
    [store.tenantProjects, tenantId],
  );
  const environments = useMemo(
    () =>
      projects.flatMap((project) =>
        (project.environments ?? []).map((environment) => ({
          id: environment.id,
          label: `${project.slug} / ${environment.name}`,
        })),
      ),
    [projects],
  );
  const [editing, setEditing] = useState<Runner | null>(null);
  const [slug, setSlug] = useState("");
  const [saving, setSaving] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);
  const [removing, setRemoving] = useState<Runner | null>(null);
  const [creating, setCreating] = useState(false);
  const [createSlug, setCreateSlug] = useState("");
  const [createOwner, setCreateOwner] = useState("tenant");
  const [githubUrl, setGithubUrl] = useState("");
  const [labels, setLabels] = useState("");
  const [registrationToken, setRegistrationToken] = useState("");
  const [createError, setCreateError] = useState<string | null>(null);
  const [submittingCreate, setSubmittingCreate] = useState(false);
  const [retrying, setRetrying] = useState<Runner | null>(null);
  const [retryToken, setRetryToken] = useState("");
  const [retryError, setRetryError] = useState<string | null>(null);
  const [submittingRetry, setSubmittingRetry] = useState(false);
  const [acceptedTaskId, setAcceptedTaskId] = useState<string | null>(null);

  useEffect(() => {
    if (tenantId === "") return;
    void store.refreshRunners(tenantId);
  }, [store.refreshRunners, tenantId]);

  if (!tenant) {
    if (store.tenantsLoading)
      return <EmptyState icon={<Cpu />} title="Loading tenant" />;
    return <EmptyState icon={<Cpu />} title="Tenant not found" />;
  }

  const projectLabels = new Map(
    projects.map((project) => [project.id, project.slug]),
  );
  const environmentLabels = new Map(
    environments.map((environment) => [environment.id, environment.label]),
  );
  const runners = store.runners.filter(
    (runner) => runner.tenantId === tenant.id,
  );
  const normalizedSlug = slug.trim();
  const validSlug =
    /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/.test(normalizedSlug) &&
    !normalizedSlug.includes("--");
  const slugTaken = runners.some(
    (runner) => runner.id !== editing?.id && runner.slug === normalizedSlug,
  );

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        eyebrow={`Tenant · ${tenant.name}`}
        title="GitHub Runners"
        description="Trusted CI/CD · Tenant, Project or Environment ownership."
        icon={<GitBranch />}
        actions={
          <Button
            disabled={store.runnersLoading || runners.length >= 5}
            onClick={() => {
              setCreating(true);
              setCreateError(null);
            }}
          >
            <Plus className="size-4" /> Create Runner
          </Button>
        }
      />

      {acceptedTaskId && (
        <div className="rounded-lg border border-success/30 bg-success/10 px-4 py-3 text-sm">
          Controller Task accepted:{" "}
          <TaskLink taskId={acceptedTaskId}>{acceptedTaskId}</TaskLink>
        </div>
      )}

      <RunnerInventory
        runners={runners}
        loading={store.runnersLoading}
        error={store.runnerError}
        owner={(runner) =>
          runner.environmentId
            ? {
                scope: "Environment",
                name:
                  environmentLabels.get(runner.environmentId) ??
                  runner.environmentId,
              }
            : runner.projectId
              ? {
                  scope: "Project",
                  name: projectLabels.get(runner.projectId) ?? runner.projectId,
                }
              : { scope: "Tenant", name: tenant.slug }
        }
        onRename={(runner) => {
          setEditing(runner);
          setSlug(runner.slug);
          setEditError(null);
        }}
        onRetry={(runner) => {
          setRetrying(runner);
          setRetryToken("");
          setRetryError(null);
        }}
        onRemove={setRemoving}
      />

      <Dialog
        open={creating}
        onOpenChange={(open) => {
          if (!open && !submittingCreate) {
            setCreating(false);
            setRegistrationToken("");
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Runner</DialogTitle>
            <DialogDescription>
              Register one GitHub organization or repository Runner. The
              short-lived token is sent once and never stored.
            </DialogDescription>
          </DialogHeader>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              const normalizedLabels = labels
                .split(",")
                .map((label) => label.trim())
                .filter(Boolean);
              if (!createSlug.trim() || !githubUrl.trim() || !registrationToken)
                return;
              void (async () => {
                setSubmittingCreate(true);
                setCreateError(null);
                try {
                  const taskId = await store.createRunner({
                    slug: createSlug.trim(),
                    tenantId: createOwner === "tenant" ? tenant.id : undefined,
                    projectId: projects.some(
                      (project) => project.id === createOwner,
                    )
                      ? createOwner
                      : undefined,
                    environmentId: environments.some(
                      (environment) => environment.id === createOwner,
                    )
                      ? createOwner
                      : undefined,
                    githubUrl: githubUrl.trim(),
                    labels: normalizedLabels,
                    registrationToken,
                  });
                  setAcceptedTaskId(taskId);
                  setRegistrationToken("");
                  setCreating(false);
                  await store.refreshRunners(tenant.id);
                } catch (error) {
                  setCreateError(
                    error instanceof Error
                      ? error.message
                      : "Unable to create Runner",
                  );
                } finally {
                  setSubmittingCreate(false);
                }
              })();
            }}
          >
            <fieldset className="space-y-4 rounded-lg border border-border p-4">
              <legend className="px-2 text-xs font-medium">Ownership</legend>
              <div className="space-y-2">
                <Label htmlFor="runner-create-slug">Slug</Label>
                <Input
                  id="runner-create-slug"
                  value={createSlug}
                  onChange={(event) => setCreateSlug(event.target.value)}
                  autoFocus
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="runner-create-owner">Owner</Label>
                <Select
                  id="runner-create-owner"
                  value={createOwner}
                  onValueChange={setCreateOwner}
                  options={[
                    { value: "tenant", label: `Tenant · ${tenant.slug}` },
                    ...projects.map((project) => ({
                      value: project.id,
                      label: `Project · ${project.slug}`,
                    })),
                    ...environments.map((environment) => ({
                      value: environment.id,
                      label: `Environment · ${environment.label}`,
                    })),
                  ]}
                />
                <p className="text-xs text-muted-foreground">
                  Owner is permanent. Trusted workflows use normal GP CLI/API
                  access.
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="runner-create-github">GitHub URL</Label>
                <Input
                  id="runner-create-github"
                  value={githubUrl}
                  onChange={(event) => setGithubUrl(event.target.value)}
                  placeholder="https://github.com/example/repository"
                />
              </div>
            </fieldset>
            <fieldset className="space-y-4 rounded-lg border border-border p-4">
              <legend className="px-2 text-xs font-medium">Registration</legend>
              <div className="space-y-2">
                <Label htmlFor="runner-create-labels">Labels</Label>
                <Input
                  id="runner-create-labels"
                  value={labels}
                  onChange={(event) => setLabels(event.target.value)}
                  placeholder="build, deployment"
                />
                <p className="text-xs text-muted-foreground">
                  Optional, comma-separated. Groundplane adds the standard
                  GitHub labels.
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="runner-create-token">Registration token</Label>
                <Input
                  id="runner-create-token"
                  type="password"
                  autoComplete="off"
                  value={registrationToken}
                  onChange={(event) => setRegistrationToken(event.target.value)}
                />
                {createError && (
                  <p className="text-sm text-destructive">{createError}</p>
                )}
              </div>
            </fieldset>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={submittingCreate}
                onClick={() => {
                  setCreating(false);
                  setRegistrationToken("");
                }}
              >
                Cancel
              </Button>
              <Button
                type="submit"
                disabled={
                  submittingCreate ||
                  !createSlug.trim() ||
                  !githubUrl.trim() ||
                  !registrationToken
                }
              >
                {submittingCreate ? "Dispatching..." : "Create Runner"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog
        open={retrying !== null}
        onOpenChange={(open) => {
          if (!open && !submittingRetry) {
            setRetrying(null);
            setRetryToken("");
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Retry Runner creation</DialogTitle>
            <DialogDescription>
              Reuse the Runner id, quota, host slot, and subnet with a fresh
              short-lived GitHub registration token.
            </DialogDescription>
          </DialogHeader>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              if (!retrying || !retryToken) return;
              void (async () => {
                setSubmittingRetry(true);
                setRetryError(null);
                try {
                  const taskId = await store.retryRunner(
                    retrying.id,
                    retryToken,
                  );
                  setAcceptedTaskId(taskId);
                  setRetryToken("");
                  setRetrying(null);
                  await store.refreshRunners(tenant.id);
                } catch (error) {
                  setRetryError(
                    error instanceof Error
                      ? error.message
                      : "Unable to retry Runner",
                  );
                } finally {
                  setSubmittingRetry(false);
                }
              })();
            }}
          >
            <div className="space-y-2">
              <Label htmlFor="runner-retry-token">
                Fresh registration token
              </Label>
              <Input
                id="runner-retry-token"
                type="password"
                autoComplete="off"
                value={retryToken}
                onChange={(event) => setRetryToken(event.target.value)}
                autoFocus
              />
              {retryError && (
                <p className="text-sm text-destructive">{retryError}</p>
              )}
            </div>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={submittingRetry}
                onClick={() => {
                  setRetrying(null);
                  setRetryToken("");
                }}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={submittingRetry || !retryToken}>
                {submittingRetry ? "Dispatching..." : "Retry Runner"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog
        open={editing !== null}
        onOpenChange={(open) => {
          if (!open && !saving) setEditing(null);
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Runner slug</DialogTitle>
            <DialogDescription>
              Changes only the Groundplane label. GitHub name, registration,
              owner, and runtime stay unchanged.
            </DialogDescription>
          </DialogHeader>
          <form
            className="space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              if (
                !editing ||
                !validSlug ||
                slugTaken ||
                normalizedSlug === editing.slug
              )
                return;
              void (async () => {
                setSaving(true);
                setEditError(null);
                try {
                  await store.renameRunner(editing.id, normalizedSlug);
                  setEditing(null);
                } catch (error) {
                  setEditError(
                    error instanceof Error
                      ? error.message
                      : "Unable to edit Runner slug",
                  );
                } finally {
                  setSaving(false);
                }
              })();
            }}
          >
            <div className="space-y-2">
              <Label htmlFor="runner-slug">Slug</Label>
              <Input
                id="runner-slug"
                value={slug}
                onChange={(event) => setSlug(event.target.value)}
                autoFocus
              />
              {slugTaken && (
                <p className="text-sm text-destructive">
                  This Tenant already has a Runner with that slug.
                </p>
              )}
              {editError && (
                <p className="text-sm text-destructive">{editError}</p>
              )}
            </div>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                disabled={saving}
                onClick={() => setEditing(null)}
              >
                Cancel
              </Button>
              <Button
                type="submit"
                disabled={
                  saving ||
                  !validSlug ||
                  slugTaken ||
                  normalizedSlug === editing?.slug
                }
              >
                {saving ? "Saving..." : "Save slug"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
      <TaskRunnerDialog
        open={removing !== null}
        onOpenChange={(open) => {
          if (!open) setRemoving(null);
        }}
        title={`Remove Runner · ${removing?.slug ?? ""}`}
        description="Stop and remove the local Runner runtime, then release its Tenant quota, dedicated subnet, host identity, and record. GitHub deregistration remains manual."
        type="remove"
        target={removing?.id ?? tenant.id}
        workspace={params.tenant}
        destructive
        confirmText={removing?.slug ?? ""}
        startLabel="Remove Runner"
        executionCopy="The Controller will remove this managed Runner:"
        steps={[
          {
            label: "Stop and remove the local Runner runtime",
            state: "pending",
          },
          {
            label: "Release Runner record and reserved allocations",
            state: "pending",
          },
        ]}
        onDispatch={() =>
          removing
            ? store.removeRunner(removing.id)
            : Promise.reject(new Error("No Runner selected"))
        }
        onCommit={() => {
          setRemoving(null);
          void store.refreshRunners(tenant.id).catch(() => undefined);
        }}
      />
    </div>
  );
}
