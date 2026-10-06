import type { LucideIcon } from "lucide-react";
import { Link } from "react-router-dom";
import { cn } from "@/lib/utils";

export type NavigationGroup = {
  label: string;
  items: { label: string; href: string; icon: LucideIcon; active?: boolean }[];
};

export function WorkspaceNavigation({
  label,
  groups,
}: {
  label: string;
  groups: NavigationGroup[];
}) {
  return (
    <nav aria-label={label} className="space-y-6">
      {groups.map((group) => (
        <section
          key={group.label}
          className="space-y-1 border-t border-border pt-5 first:border-t-0 first:pt-0"
        >
          <h2 className="px-3 pb-2 text-[12px] font-semibold uppercase tracking-wider text-muted-foreground [overflow-wrap:anywhere]">
            {group.label}
          </h2>
          {group.items.map((item) => (
            <Link
              key={item.href}
              to={item.href}
              aria-current={item.active ? "page" : undefined}
              className={cn(
                "flex min-h-10 items-start gap-3 rounded-r-md rounded-l-sm border-l-2 border-transparent px-3 py-2.5 text-sm leading-5 outline-none transition-colors hover:bg-sidebar-accent focus-visible:ring-2 focus-visible:ring-ring [overflow-wrap:anywhere]",
                item.active
                  ? "border-l-primary bg-accent font-semibold text-accent-foreground"
                  : "text-sidebar-foreground hover:text-foreground",
              )}
            >
              <item.icon
                aria-hidden="true"
                className="mt-0.5 size-4 shrink-0 text-muted-foreground"
              />
              <span className="min-w-0">{item.label}</span>
            </Link>
          ))}
        </section>
      ))}
    </nav>
  );
}
