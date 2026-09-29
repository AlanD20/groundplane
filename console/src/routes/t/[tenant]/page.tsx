import { ProjectsTable } from "@/features/project/projects-table";

import { EmptyState } from "@/components/common/empty-state";
import { HierarchyDeleteDialog } from "@/components/common/hierarchy-delete-dialog";
import { MetaPill } from "@/components/common/meta-pill";
import { PageHeader } from "@/components/common/page-header";
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
import { useLinkedSlug } from "@/lib/use-linked-slug";
import { Boxes, Cpu, Plus, ShieldCheck } from "lucide-react";
import { useState } from "react";

export default function TenantPage() {
  const params = useRequiredParams("tenant");
  const store = useStore();
  const tenant = store.getTenant(params.tenant);
  const projects = store.tenantProjects.filter(
    (p) => p.tenantId === tenant?.id,
  );
  const runners = store.runners.filter((r) => r.tenantId === tenant?.id);

  const [open, setOpen] = useState(false);
  const {
    name,
    slug,
    setName,
    setSlug,
    reset: resetProjectIdentity,
  } = useLinkedSlug();
  const [description, setDescription] = useState("");
  const [projectError, setProjectError] = useState<string | null>(null);
  const [creatingProject, setCreatingProject] = useState(false);
  const [removingProject, setRemovingProject] = useState<
    (typeof projects)[number] | null
  >(null);

  if (!tenant) {
    if (store.tenantsLoading)
      return <EmptyState icon={<Boxes />} title="Loading tenant" />;
    return (
      <EmptyState
        icon={<Boxes />}
        title="Tenant not found"
        description={store.tenantError ?? `${params.tenant} does not exist.`}
      />
    );
  }

  return (
    <div className="flex flex-col gap-6">
      <PageHeader
        title={tenant.name}
        description={tenant.description}
        icon={<Boxes />}
        meta={
          <>
            <MetaPill icon={<Boxes />}>{projects.length} projects</MetaPill>
            <MetaPill icon={<Cpu />}>
              {runners.length} runners ·{" "}
              {runners.filter((r) => r.online).length} online
            </MetaPill>
            <MetaPill tone="success" icon={<ShieldCheck />}>
              isolated
            </MetaPill>
          </>
        }
        actions={
          <Button onClick={() => setOpen(true)}>
            <Plus className="size-4" /> New project
          </Button>
        }
      />

      {projects.length === 0 ? (
        <EmptyState
          icon={<Boxes />}
          title="No projects yet"
          description="A project is an application — it may be a microservice architecture with many services."
          action={
            <Button onClick={() => setOpen(true)}>
              <Plus className="size-4" /> New project
            </Button>
          }
        />
      ) : (
        <ProjectsTable
          tenant={tenant}
          projects={projects}
          onRemove={setRemovingProject}
        />
      )}

      <Drawer open={open} onOpenChange={setOpen}>
        <DrawerContent>
          <DialogHeader>
            <DialogTitle>New project · {tenant.name}</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4">
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="p-name">Name</Label>
              <Input
                id="p-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="checkout"
                autoFocus
              />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="p-slug">Slug</Label>
              <Input
                id="p-slug"
                value={slug}
                onChange={(e) => setSlug(e.target.value)}
                placeholder="checkout"
              />
              <p className="text-xs text-muted-foreground">
                Follows Name until edited.
              </p>
            </div>
            <div className="flex flex-col gap-1.5">
              <Label htmlFor="p-desc">Description</Label>
              <Input
                id="p-desc"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What this project is"
              />
            </div>
            <p className="text-xs text-muted-foreground">
              A project may be a microservice architecture. Environments
              (staging, production) are created inside it, and each environment
              attaches backing services.
            </p>
            {projectError && (
              <p role="alert" className="text-xs text-destructive">
                {projectError}
              </p>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={creatingProject || !name.trim() || !slug.trim()}
              onClick={() =>
                void (async () => {
                  setCreatingProject(true);
                  setProjectError(null);
                  try {
                    await store.addProject({
                      tenantId: tenant.id,
                      slug,
                      name: name.trim(),
                      description: description.trim(),
                    });
                    setOpen(false);
                    resetProjectIdentity();
                    setDescription("");
                  } catch (error) {
                    setProjectError(
                      error instanceof Error
                        ? error.message
                        : "Unable to create project",
                    );
                  } finally {
                    setCreatingProject(false);
                  }
                })()
              }
            >
              {creatingProject ? "Creating…" : "Create project"}
            </Button>
          </DialogFooter>
        </DrawerContent>
      </Drawer>

      {removingProject ? (
        <HierarchyDeleteDialog
          open
          onOpenChange={(open) => !open && setRemovingProject(null)}
          kind="project"
          name={removingProject.slug}
          id={removingProject.id}
          workspace={tenant.slug}
          description="Permanently removes this Project and every Environment, workload, volume, Secret, Connector, and project Runner it owns."
          steps={[
            { label: "Freeze Project membership", state: "pending" },
            { label: "Remove child runtime and storage", state: "pending" },
            { label: "Remove Environment state", state: "pending" },
            { label: "Remove the Project", state: "pending" },
          ]}
          onDispatch={() => store.deleteProject(removingProject.id)}
          onCommit={() => setRemovingProject(null)}
        />
      ) : null}
    </div>
  );
}
