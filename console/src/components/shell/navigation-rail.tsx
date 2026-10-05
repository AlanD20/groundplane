import { ThemeToggle } from "@/components/common/theme-toggle";
import { cn } from "@/lib/utils";
import { Activity, Database, Layers, Server, Settings } from "lucide-react";
import { Link } from "react-router-dom";

export function NavigationRail({ pathname }: { pathname: string }) {
  const links = [
    {
      label: "Projects",
      href: "/platform/overview",
      icon: Layers,
      active: pathname.startsWith("/t/") || pathname === "/platform/overview",
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
      label: "Activity",
      href: "/platform/activity",
      icon: Activity,
      active: pathname === "/platform/activity",
    },
    {
      label: "Settings",
      href: "/platform/settings",
      icon: Settings,
      active:
        pathname.startsWith("/platform/settings") ||
        pathname === "/platform/secrets",
    },
  ];
  return (
    <nav
      aria-label="Platform navigation"
      className="flex h-full w-16 shrink-0 flex-col items-center border-r border-border bg-sidebar py-5"
    >
      <Link
        to="/platform/overview"
        aria-label="Groundplane"
        title="Groundplane"
        className="mb-10 flex size-10 items-center justify-center rounded-lg text-primary"
      >
        <Layers className="size-6" />
      </Link>
      <div className="flex flex-col gap-3">
        {links.map(({ label, href, icon: Icon, active }) => (
          <Link
            key={href}
            to={href}
            title={label}
            aria-label={label}
            aria-current={active ? "page" : undefined}
            className={cn(
              "flex size-10 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground",
              active && "bg-sidebar-accent text-primary",
            )}
          >
            <Icon className="size-5" />
          </Link>
        ))}
      </div>
      <div className="mt-auto">
        <ThemeToggle iconOnly />
      </div>
    </nav>
  );
}
