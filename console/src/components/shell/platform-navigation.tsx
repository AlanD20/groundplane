import { cn } from "@/lib/utils";
import {
  Activity,
  Database,
  KeyRound,
  Layers,
  Server,
  Settings,
} from "lucide-react";
import { Link } from "react-router-dom";

export function PlatformNavigation({ pathname }: { pathname: string }) {
  const links = [
    {
      label: "Overview",
      href: "/platform/overview",
      icon: Layers,
      active: pathname === "/platform/overview",
    },
    {
      label: "Backing Services",
      href: "/platform/backing-services",
      icon: Database,
      active: pathname.startsWith("/platform/backing-services"),
    },
    {
      label: "Host",
      href: "/platform/host",
      icon: Server,
      active:
        pathname.startsWith("/platform/host") ||
        pathname.startsWith("/platform/components"),
    },
    {
      label: "Tasks",
      href: "/platform/activity",
      icon: Activity,
      active: pathname === "/platform/activity",
    },
    {
      label: "Secrets",
      href: "/platform/secrets",
      icon: KeyRound,
      active: pathname === "/platform/secrets",
    },
    {
      label: "Preferences",
      href: "/platform/settings",
      icon: Settings,
      active: pathname.startsWith("/platform/settings"),
    },
  ];
  return (
    <nav
      aria-label="Platform navigation"
      className="grid grid-cols-2 gap-1 border-b border-border p-3"
    >
      {links.map(({ label, href, icon: Icon, active }) => (
        <Link
          key={href}
          to={href}
          aria-current={active ? "page" : undefined}
          className={cn(
            "flex min-w-0 items-center gap-2 rounded-md px-2 py-2 text-xs outline-none hover:bg-sidebar-accent focus-visible:ring-2 focus-visible:ring-ring",
            active
              ? "bg-accent text-primary"
              : "text-muted-foreground hover:text-foreground",
          )}
        >
          <Icon className="size-4 shrink-0" />
          <span>{label}</span>
        </Link>
      ))}
    </nav>
  );
}
