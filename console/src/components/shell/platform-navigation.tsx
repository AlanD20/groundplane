import { Tooltip } from "@/components/ui/tooltip";
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

export function PlatformNavigation({
  pathname,
  compact = true,
}: {
  pathname: string;
  compact?: boolean;
}) {
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
      className={cn(
        "flex shrink-0 flex-col gap-3 overflow-y-auto bg-sidebar py-4",
        compact ? "w-16 items-center border-r border-border" : "w-full px-4",
      )}
    >
      {compact && (
        <div
          aria-hidden="true"
          className="mb-5 flex size-10 shrink-0 items-center justify-center text-primary"
        >
          <Layers className="size-6" />
        </div>
      )}
      {links.map(({ label, href, icon: Icon, active }) => (
        <Tooltip key={href} content={label} side="right">
          <Link
            aria-label={label}
            to={href}
            aria-current={active ? "page" : undefined}
            className={cn(
              "flex min-h-10 shrink-0 items-center gap-3 rounded-lg outline-none hover:bg-sidebar-accent focus-visible:ring-2 focus-visible:ring-ring",
              compact ? "size-10 justify-center" : "px-3 text-sm",
              active
                ? "bg-accent text-primary"
                : "text-muted-foreground hover:text-foreground",
            )}
          >
            <Icon aria-hidden="true" className="size-5 shrink-0" />
            {!compact && <span>{label}</span>}
          </Link>
        </Tooltip>
      ))}
    </nav>
  );
}
