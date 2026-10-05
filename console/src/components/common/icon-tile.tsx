import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

export function IconTile({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <span
      className={cn(
        "grid size-8.5 shrink-0 place-items-center rounded-lg border border-border bg-accent text-primary [&_svg]:size-4",
        className,
      )}
    >
      {children}
    </span>
  );
}
