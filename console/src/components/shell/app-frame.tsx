"use client";

import { ThemeToggle } from "@/components/common/theme-toggle";
import { TaskNotifications } from "@/features/task/task-access";
import { Brand } from "./brand";

import { Button } from "@/components/ui/button";
import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { TooltipProvider } from "@/components/ui/tooltip";
import { useStore } from "@/lib/store";
import { useLinkedSlug } from "@/lib/use-linked-slug";
import { Menu } from "lucide-react";
import { useEffect, useState } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { Sidebar } from "./sidebar";
import { Topbar } from "./topbar";
import { WorkspaceSwitcher } from "./workspace-switcher";
import { NavigationRail } from "./navigation-rail";

function useWorkspace(pathname: string): {
  kind: "platform" | "tenant";
  slug: string;
} {
  const parts = pathname.split("/").filter(Boolean);
  if (parts[0] === "t" && parts[1]) return { kind: "tenant", slug: parts[1] };
  return { kind: "platform", slug: "platform" };
}

// The project the current route lives under, if any (/t/<tenant>/<project>/…).
// parts[2] is only a project when it actually names one — tenant-level
// sections (runners, activity, settings) must never trigger the project nav.
function useProjectSlug(
  pathname: string,
  projectSlugs: string[],
): string | undefined {
  const parts = pathname.split("/").filter(Boolean);
  if (
    parts[0] === "t" &&
    parts[1] &&
    parts[2] &&
    projectSlugs.includes(parts[2])
  )
    return parts[2];
  return undefined;
}

export function AppFrame({ children }: { children: React.ReactNode }) {
  const { pathname, search } = useLocation();
  const workspace = useWorkspace(pathname);
  const navigate = useNavigate();
  const { addTenant, tenantProjects } = useStore();
  const projectSlug = useProjectSlug(
    pathname,
    tenantProjects.map((p) => p.slug),
  );

  const [mobileOpen, setMobileOpen] = useState(false);
  useEffect(() => setMobileOpen(false), [pathname, search]);
  const [newTenantOpen, setNewTenantOpen] = useState(false);
  const {
    name,
    slug,
    setName,
    setSlug,
    reset: resetTenantIdentity,
  } = useLinkedSlug();
  const [description, setDescription] = useState("");
  const [createError, setCreateError] = useState<string | null>(null);
  const [creating, setCreating] = useState(false);

  async function createTenant() {
    const s = slug.trim();
    if (creating || !s) return;
    setCreating(true);
    setCreateError(null);
    try {
      const tenant = await addTenant({
        slug: s,
        name: name.trim() || s,
        description: description.trim(),
      });
      setNewTenantOpen(false);
      resetTenantIdentity();
      setDescription("");
      navigate(`/t/${tenant.slug}`);
    } catch (error) {
      setCreateError(
        error instanceof Error ? error.message : "Unable to create tenant",
      );
    } finally {
      setCreating(false);
    }
  }

  return (
    <TooltipProvider>
      <div className="flex min-h-screen w-full">
        {/* Desktop sidebar */}
        <aside className="fixed inset-y-0 left-0 z-30 hidden w-[294px] border-r border-border bg-sidebar lg:flex">
          <NavigationRail pathname={pathname} />
          <div className="flex min-w-0 flex-1 flex-col">
            <div className="flex h-16 shrink-0 items-center px-4">
              <WorkspaceSwitcher
                workspace={workspace}
                onNewTenant={() => setNewTenantOpen(true)}
              />
            </div>
            <div className="flex-1 overflow-y-auto">
              <Sidebar
                workspace={workspace}
                projectSlug={projectSlug}
                pathname={pathname}
              />
            </div>
          </div>
        </aside>

        <Drawer open={mobileOpen} onOpenChange={setMobileOpen}>
          <DrawerContent
            className="left-0 right-auto max-w-80 border-l-0 border-r p-0 data-[ending-style]:-translate-x-full data-[starting-style]:-translate-x-full"
            onClick={(event) => {
              if (event.target instanceof Element && event.target.closest("a"))
                setMobileOpen(false);
            }}
          >
            <DialogTitle className="sr-only">Navigation</DialogTitle>
            <div className="border-b border-border p-4">
              <Brand />
            </div>
            <div className="flex min-h-0 flex-1">
              <NavigationRail pathname={pathname} />
              <div className="flex min-w-0 flex-1 flex-col">
                <div className="px-3">
                  <WorkspaceSwitcher
                    workspace={workspace}
                    onNewTenant={() => {
                      setMobileOpen(false);
                      setNewTenantOpen(true);
                    }}
                  />
                </div>
                <div className="flex-1 overflow-y-auto">
                  <Sidebar
                    workspace={workspace}
                    projectSlug={projectSlug}
                    pathname={pathname}
                  />
                </div>
              </div>
            </div>
          </DrawerContent>
        </Drawer>

        {/* Main column */}
        <div className="flex min-w-0 flex-1 flex-col lg:pl-[294px]">
          <div className="flex min-h-16 items-center gap-2 border-b border-border px-4 sm:px-6 lg:px-7">
            <Button
              variant="outline"
              size="icon"
              onClick={() => setMobileOpen(true)}
              className="shrink-0 text-muted-foreground lg:hidden"
            >
              <Menu className="size-5" />
              <span className="sr-only">Open menu</span>
            </Button>
            <div className="min-w-0 flex-1">
              <Topbar pathname={pathname} />
            </div>
            <TaskNotifications />
            <div className="lg:hidden">
              <ThemeToggle />
            </div>
          </div>
          <main className="mx-auto w-full max-w-[1580px] flex-1 px-4 pb-12 pt-6 sm:px-6 lg:px-7 console-content">
            <div className="console-page" key={pathname}>
              {children}
            </div>
          </main>
        </div>
      </div>

      <Drawer open={newTenantOpen} onOpenChange={setNewTenantOpen}>
        <DrawerContent>
          <form
            className="flex flex-col gap-5"
            onSubmit={(event) => {
              event.preventDefault();
              if (name.trim() && slug.trim()) void createTenant();
            }}
          >
            <DialogHeader>
              <DialogTitle>New tenant</DialogTitle>
            </DialogHeader>
            <div className="flex flex-col gap-4">
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="t-name">Display name</Label>
                <Input
                  id="t-name"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="Acme Inc."
                  autoFocus
                />
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="t-slug">Slug</Label>
                <Input
                  id="t-slug"
                  value={slug}
                  onChange={(e) => setSlug(e.target.value)}
                  placeholder="acme"
                />
                <p className="text-xs text-muted-foreground">
                  Follows Display name until edited. Projects, runners, and
                  secrets live under this isolation boundary.
                </p>
              </div>
              <div className="flex flex-col gap-1.5">
                <Label htmlFor="t-desc">Description</Label>
                <Input
                  id="t-desc"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="What this tenant hosts"
                />
              </div>
              {createError && (
                <p role="alert" className="text-xs text-destructive">
                  {createError}
                </p>
              )}
            </div>
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => setNewTenantOpen(false)}
              >
                Cancel
              </Button>
              <Button
                type="submit"
                disabled={creating || !name.trim() || !slug.trim()}
              >
                {creating ? "Creating…" : "Create tenant"}
              </Button>
            </DialogFooter>
          </form>
        </DrawerContent>
      </Drawer>
    </TooltipProvider>
  );
}
