import type { ReactNode } from "react";
import { Link, useLocation } from "react-router-dom";
import { ArrowUpRight, ExternalLink } from "lucide-react";
import {
  ContextMenu,
  ContextMenuTrigger,
  ContextMenuContent,
  ContextMenuItem,
} from "@/components/ui/context-menu";
import type { TopologyNode } from "./topology-model";

export function TopologyNodeMenu({
  node,
  children,
}: {
  node: TopologyNode;
  children: ReactNode;
}) {
  const location = useLocation();
  let href: string;
  let label: string;
  if (node.kind === "Service") {
    const search = new URLSearchParams(location.search);
    search.set("view", "overview");
    search.set("panel", "overview");
    search.set("service", node.id);
    search.set("serviceTab", "overview");
    href = `${location.pathname}?${search}`;
    label = "Open Service";
  } else if (node.kind === "Backing" && node.backingProjectId) {
    href = `/platform/backing-services/${encodeURIComponent(node.backingProjectId)}`;
    label = "Open Backing Service";
  } else {
    return children;
  }
  return (
    <ContextMenu>
      <ContextMenuTrigger className="h-full">{children}</ContextMenuTrigger>
      <ContextMenuContent
        onPointerDown={(event) => event.stopPropagation()}
        onKeyDown={(event) => event.stopPropagation()}
      >
        <ContextMenuItem render={<Link to={href} />}>
          <ArrowUpRight /> {label}
        </ContextMenuItem>
        <ContextMenuItem
          render={<Link to={href} target="_blank" rel="noopener noreferrer" />}
        >
          <ExternalLink /> Open in new tab
        </ContextMenuItem>
      </ContextMenuContent>
    </ContextMenu>
  );
}
