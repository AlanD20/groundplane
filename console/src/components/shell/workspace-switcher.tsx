"use client";

import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useStore } from "@/lib/store";
import { Boxes, Check, ChevronsUpDown, Globe, Plus } from "lucide-react";
import { useLocation, useNavigate } from "react-router-dom";

// The primary selection (Vercel team / Neon org style): the dropdown holds
// Platform (the workspace) and every Tenant; the sidebar and every page
// recontextualize to the selection.
export function WorkspaceSwitcher({
  workspace,
  onNewTenant,
}: {
  workspace: { kind: "platform" | "tenant"; slug: string };
  onNewTenant: () => void;
}) {
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const { tenants } = useStore();
  const current =
    workspace.kind === "platform"
      ? {
          name: pathname.startsWith("/platform/backing-services")
            ? "Shared services"
            : pathname.startsWith("/platform/host") ||
                pathname.startsWith("/platform/components")
              ? "Host"
              : pathname.startsWith("/platform/settings") ||
                  pathname === "/platform/secrets"
                ? "Settings"
                : pathname === "/platform/activity"
                  ? "Activity"
                  : "Projects",
        }
      : (() => {
          const t = tenants.find((x) => x.slug === workspace.slug);
          return {
            name: t?.name ?? workspace.slug,
          };
        })();

  return (
    <DropdownMenu>
      <DropdownMenuTrigger className="flex w-full items-center gap-2 rounded-lg px-2 py-3 text-left outline-none transition-colors hover:bg-muted focus-visible:ring-1 focus-visible:ring-ring">
        <span className="flex min-w-0 flex-1 flex-col">
          <span className="truncate text-sm font-semibold leading-tight">
            {current.name}
          </span>
        </span>
        <ChevronsUpDown className="size-4 shrink-0 text-muted-foreground" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start" className="w-64">
        <DropdownMenuLabel>Workspace</DropdownMenuLabel>
        <DropdownMenuItem onClick={() => navigate("/platform/overview")}>
          <Globe />
          <span className="flex-1">Platform</span>
          {workspace.kind === "platform" && (
            <Check className="size-4 text-primary" />
          )}
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuLabel>Tenants</DropdownMenuLabel>
        {tenants.map((t) => (
          <DropdownMenuItem key={t.id} onClick={() => navigate(`/t/${t.slug}`)}>
            <Boxes />
            <span className="flex-1 truncate">{t.name}</span>
            {workspace.kind === "tenant" && workspace.slug === t.slug && (
              <Check className="size-4 text-primary" />
            )}
          </DropdownMenuItem>
        ))}
        <DropdownMenuSeparator />
        <DropdownMenuItem onClick={onNewTenant}>
          <Plus />
          New tenant
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
