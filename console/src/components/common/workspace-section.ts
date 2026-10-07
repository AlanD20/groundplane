import { cn } from "@/lib/utils";

/** One resting row and one active-edit treatment across resource workspaces. */
export function workspaceSectionClassName(editing = false, className?: string) {
  return cn(
    "min-w-0 border p-4 transition-colors motion-reduce:transition-none",
    editing
      ? "rounded-lg border-primary/40 bg-primary/5"
      : "rounded-none border-transparent border-b-border last:border-b-transparent",
    className,
  );
}

export const editorFooterClassName = "mt-4 border-t border-border pt-4";
