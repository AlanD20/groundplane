import { EnvironmentsTable } from "@/features/environment/environments-table";

import { EmptyState } from "@/components/common/empty-state";
import { HierarchyDeleteDialog } from "@/components/common/hierarchy-delete-dialog";
import { MetaPill } from "@/components/common/meta-pill";
import { PageHeader } from "@/components/common/page-header";
import { StatusDot } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment } from "@/lib/types";
import {
  ArrowLeft,
  Boxes,
  Building2,
  KeyRound,
  Layers,
  Plus,
} from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";

export default function TenantProjectPage() {
  const params = useRequiredParams("tenant", "project");
  const store = useStore();
  const tenant = store.getTenant(params.tenant);
  const project = store.getProject(params.tenant, params.project);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [networkPool, setNetworkPool] = useState("");
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string>();
  const [removingEnvironment, setRemovingEnvironment] =
    useState<Environment | null>(null);

  if (!tenant || !project || project.tenantId !== tenant.id) {
    return (
      <EmptyState
        icon={<Boxes />}
        title="Project not found"
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

  const envs = project.environments ?? [];
  const svcCount = envs.reduce((n, e) => n + e.services.length, 0);
  const repoRunner = store.runners.find(
    (r) => r.tenantId === tenant.id && r.projectId === project.id,
  );

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={
          <>
            {project.name}
            {repoRunner && (
              <span className="ml-2 inline-flex items-center gap-1.5 rounded-full border border-border bg-surface px-3 py-1 align-middle text-xs text-muted-foreground">
                <StatusDot status={repoRunner.online ? "healthy" : "stopped"} />
                Runner · {repoRunner.labels.join(", ") ||
                  "GitHub Actions"} ·{" "}
                <span className={repoRunner.online ? "text-success" : ""}>
                  {repoRunner.online ? "online" : "offline"}
                </span>
              </span>
            )}
          </>
        }
        eyebrow={`Tenant · ${tenant.name} / Project`}
        description={project.description}
        icon={<Boxes />}
        meta={
          <>
            <MetaPill icon={<Building2 />}>{tenant.name}</MetaPill>
            <MetaPill icon={<Layers />}>
              {envs.length} environment{envs.length === 1 ? "" : "s"}
            </MetaPill>
            <MetaPill icon={<Boxes />}>{svcCount} services</MetaPill>
          </>
        }
        actions={
          <>
            <Link to={`/t/${tenant.slug}/${project.slug}/secrets`}>
              <Button variant="outline">
                <KeyRound className="size-4" /> Secrets
              </Button>
            </Link>
            <Button onClick={() => setOpen(true)}>
              <Plus className="size-4" /> New environment
            </Button>
          </>
        }
      />

      {/* Environment selector */}
      {envs.length === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No environments yet"
          description="An environment is one deployable instance of this project — staging, production, anything."
          action={
            <Button onClick={() => setOpen(true)}>
              <Plus className="size-4" /> New environment
            </Button>
          }
        />
      ) : (
        <EnvironmentsTable
          tenant={tenant}
          project={project}
          onRemove={setRemovingEnvironment}
        />
      )}

      <Drawer open={open} onOpenChange={setOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New environment · {project.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="e-name">Name</Label>
              <Input
                id="e-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="production"
                autoFocus
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="e-network-pool">Network pool</Label>
              <Input
                id="e-network-pool"
                value={networkPool}
                onChange={(e) => setNetworkPool(e.target.value)}
                placeholder="10.200.0.0/16"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              One deployable instance of the project. Public routes need the
              ingress components (Caddy + Cloudflare Tunnel), enabled on the
              Environment’s Router page — never auto-deployed.
            </p>
          </div>
          {createError && (
            <p role="alert" className="text-sm text-destructive">
              {createError}
            </p>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={creating || !name.trim() || !networkPool.trim()}
              onClick={async () => {
                const slug = name
                  .trim()
                  .toLowerCase()
                  .replace(/[^a-z0-9-]/g, "");
                setCreating(true);
                setCreateError(undefined);
                try {
                  await store.addEnvironment(
                    project.id,
                    slug,
                    networkPool.trim(),
                  );
                  setOpen(false);
                  setName("");
                  setNetworkPool("");
                } catch (error) {
                  setCreateError(
                    error instanceof Error
                      ? error.message
                      : "Unable to create Environment",
                  );
                } finally {
                  setCreating(false);
                }
              }}
            >
              {creating ? "Creating…" : "Create environment"}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      {removingEnvironment ? (
        <HierarchyDeleteDialog
          open
          onOpenChange={(open) => !open && setRemovingEnvironment(null)}
          kind="environment"
          name={removingEnvironment.name}
          id={removingEnvironment.id}
          workspace={tenant.slug}
          description="Permanently removes this Environment and every workload, volume, Entry, Attach, backup, recovery point, and Component state it owns."
          steps={[
            { label: "Freeze Environment membership", state: "pending" },
            { label: "Remove workload runtime and storage", state: "pending" },
            { label: "Remove Environment-scoped state", state: "pending" },
            { label: "Remove the Environment", state: "pending" },
          ]}
          onDispatch={() => store.deleteEnvironment(removingEnvironment.id)}
          onCommit={() => setRemovingEnvironment(null)}
        />
      ) : null}
    </div>
  );
}
