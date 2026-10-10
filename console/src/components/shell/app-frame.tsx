import { FormActionsProvider } from "@/components/common/form-actions";
import { DialogTitle } from "@/components/ui/dialog";
import { ContextReturnLink } from "@/components/common/context-link";
("use client");

import { ThemeToggle } from "@/components/common/theme-toggle";
import { TaskNotifications } from "@/features/task/task-access";
import { Brand } from "./brand";

import { Button } from "@/components/ui/button";

import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { TooltipProvider } from "@/components/ui/tooltip";
import { useStore } from "@/lib/store";
import { Menu, PanelLeftClose, PanelLeftOpen } from "lucide-react";
import { useEffect, useState, type CSSProperties } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { cn } from "@/lib/utils";
import { Sidebar, hasWorkspaceNavigation } from "./sidebar";
import { Topbar } from "./topbar";
import { WorkspaceSwitcher } from "./workspace-switcher";
import { PlatformNavigation } from "./platform-navigation";

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
  const hasLocalNavigation = hasWorkspaceNavigation(pathname);
  const navigate = useNavigate();
  const { tenantProjects, resourceRefreshErrors, retryResourceRefresh } =
    useStore();
  const projectSlug = useProjectSlug(
    pathname,
    tenantProjects.map((p) => p.slug),
  );

  const [collapsed, setCollapsed] = useState(() => {
    try {
      return (
        localStorage.getItem("groundplane-secondary-nav-collapsed") === "true"
      );
    } catch {
      return false;
    }
  });
  const toggleNavigation = () =>
    setCollapsed((current) => {
      try {
        localStorage.setItem(
          "groundplane-secondary-nav-collapsed",
          String(!current),
        );
      } catch {
        /* Preference storage may be unavailable. */
      }
      return !current;
    });
  const [mobileOpen, setMobileOpen] = useState(false);
  useEffect(() => setMobileOpen(false), [pathname, search]);

  return (
    <TooltipProvider>
      <a
        href="#console-main"
        className="sr-only fixed left-4 top-4 z-[100] rounded-md bg-popover p-3 text-sm focus:not-sr-only"
      >
        Skip to content
      </a>
      <div
        className="console-shell flex min-h-screen w-full"
        style={
          {
            "--desktop-navigation-width": hasLocalNavigation
              ? collapsed
                ? "128px"
                : "304px"
              : "64px",
          } as CSSProperties
        }
      >
        {/* Desktop sidebar */}
        <aside
          className={cn(
            "fixed inset-y-0 left-0 z-30 hidden border-r border-border bg-sidebar lg:flex",
            "w-[var(--desktop-navigation-width)]",
          )}
        >
          <PlatformNavigation pathname={pathname} />
          {hasLocalNavigation && (
            <div className="flex min-h-0 min-w-0 flex-1 flex-col">
              <div
                className={cn(
                  "flex h-16 shrink-0 items-center border-b border-border",
                  collapsed ? "justify-center px-2" : "gap-1 px-3",
                )}
              >
                {!collapsed && (
                  <div className="min-w-0 flex-1">
                    <WorkspaceSwitcher
                      workspace={workspace}
                      onNewTenant={() =>
                        navigate("/platform/overview?create=tenant")
                      }
                    />
                  </div>
                )}
                <Button
                  variant="ghost"
                  size="icon-sm"
                  aria-label={
                    collapsed
                      ? "Expand secondary navigation"
                      : "Collapse secondary navigation"
                  }
                  title={
                    collapsed
                      ? "Expand secondary navigation"
                      : "Collapse secondary navigation"
                  }
                  aria-expanded={!collapsed}
                  onClick={toggleNavigation}
                >
                  {collapsed ? <PanelLeftOpen /> : <PanelLeftClose />}
                </Button>
              </div>
              <div className="flex-1 overflow-y-auto">
                <Sidebar
                  workspace={workspace}
                  projectSlug={projectSlug}
                  pathname={pathname}
                  collapsed={collapsed}
                />
              </div>
            </div>
          )}
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
              <PlatformNavigation
                pathname={pathname}
                compact={hasLocalNavigation}
              />
              {hasLocalNavigation && (
                <div className="flex min-h-0 min-w-0 flex-1 flex-col">
                  <div className="border-b border-border px-3 py-2">
                    <WorkspaceSwitcher
                      workspace={workspace}
                      onNewTenant={() => {
                        setMobileOpen(false);
                        navigate("/platform/overview?create=tenant");
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
              )}
            </div>
          </DrawerContent>
        </Drawer>

        {/* Main column */}
        <div
          className={cn(
            "flex min-w-0 flex-1 flex-col",
            "lg:pl-[var(--desktop-navigation-width)]",
          )}
        >
          <div
            data-workspace-header
            className="flex min-h-16 items-center gap-2 border-b border-border px-4 sm:px-6 lg:px-7"
          >
            <Button
              variant="outline"
              size="sm"
              onClick={() => setMobileOpen(true)}
              className="shrink-0 text-muted-foreground lg:hidden"
            >
              <Menu className="size-5" />
              <span>Navigate</span>
            </Button>
            <div className="min-w-0 flex-1">
              <Topbar pathname={pathname} />
            </div>
            <TaskNotifications />
            <div>
              <ThemeToggle />
            </div>
          </div>
          <main
            id="console-main"
            data-local-navigation={hasLocalNavigation}
            tabIndex={-1}
            className="mx-auto w-full max-w-[1580px] flex-1 px-4 pb-12 pt-6 outline-none sm:px-6 lg:px-7 console-content"
          >
            <div className="console-page" key={pathname}>
              <ContextReturnLink />
              {resourceRefreshErrors.length > 0 && (
                <div
                  role="alert"
                  className="mb-5 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-warning/40 bg-warning/10 p-4 text-sm"
                >
                  <div>
                    <p className="font-medium">
                      Some resource details could not be refreshed after an
                      operation.
                    </p>
                    <p className="mt-1 text-xs text-muted-foreground">
                      {resourceRefreshErrors.join(" · ")}
                    </p>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={retryResourceRefresh}
                  >
                    Retry refresh
                  </Button>
                </div>
              )}
              <FormActionsProvider>{children}</FormActionsProvider>
            </div>
          </main>
        </div>
      </div>
    </TooltipProvider>
  );
}
