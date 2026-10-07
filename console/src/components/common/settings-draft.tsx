import type { ReactNode } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  editorFooterClassName,
  workspaceSectionClassName,
} from "./workspace-section";

/** A local draft boundary for settings that are edited directly on the page. */
export function SettingsDraft({
  dirty,
  busy,
  onCancel,
  actions,
  children,
  pristineLabel = "Saved configuration",
}: {
  dirty: boolean;
  busy?: boolean;
  onCancel: () => void;
  actions: ReactNode;
  children: ReactNode;
  pristineLabel?: string;
}) {
  return (
    <div className={workspaceSectionClassName(dirty)}>
      <div className="mb-4" role="status">
        <Badge variant={dirty ? "warning" : "outline"}>
          {dirty ? "Editing · Unsaved" : pristineLabel}
        </Badge>
      </div>
      <fieldset disabled={busy} className="min-w-0 space-y-4">
        {children}
      </fieldset>
      <div
        className={`${editorFooterClassName} flex flex-wrap items-center justify-end gap-2`}
      >
        <Button variant="outline" disabled={!dirty || busy} onClick={onCancel}>
          Cancel
        </Button>
        {actions}
      </div>
    </div>
  );
}
