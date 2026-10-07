import { cn } from "@/lib/utils";

/** One resting row and one active-edit treatment across resource workspaces. */
export function workspaceSectionClassName(editing = false, className?: string) {
  return cn(
    "min-w-0 border p-4 transition-colors motion-reduce:transition-none",
    editing
      ? "workspace-editing rounded-lg border-primary/40"
      : "rounded-lg border-border bg-background/30",
    className,
  );
}

export const editorFooterClassName =
  "workspace-editor-footer mt-4 border-t border-primary/25 pt-4";
