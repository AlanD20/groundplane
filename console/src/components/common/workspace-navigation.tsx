import { Link } from "react-router-dom";
import { cn } from "@/lib/utils";

export type NavigationGroup = {
  label: string;
  items: { label: string; href: string; active?: boolean }[];
};

export function WorkspaceNavigation({
  label,
  groups,
}: {
  label: string;
  groups: NavigationGroup[];
}) {
  return (
    <nav aria-label={label} className="space-y-5">
      {groups.map((group) => (
        <section key={group.label} className="space-y-1">
          <h2 className="px-3 pb-1 text-[11px] font-medium text-muted-foreground [overflow-wrap:anywhere]">
            {group.label}
          </h2>
          {group.items.map((item) => (
            <Link
              key={item.href}
              to={item.href}
              aria-current={item.active ? "page" : undefined}
              className={cn(
                "block rounded-md px-3 py-2 text-sm outline-none transition-colors hover:bg-sidebar-accent focus-visible:ring-2 focus-visible:ring-ring [overflow-wrap:anywhere]",
                item.active
                  ? "bg-accent font-medium text-primary"
                  : "text-muted-foreground hover:text-foreground",
              )}
            >
              {item.label}
            </Link>
          ))}
        </section>
      ))}
    </nav>
  );
}
