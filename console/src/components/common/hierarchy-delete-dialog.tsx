"use client";

import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";

export function HierarchyDeleteDialog({
  open,
  onOpenChange,
  kind,
  name,
  id,
  workspace,
  description,
  onDispatch,
  onCommit,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  kind: "tenant" | "project" | "environment";
  name: string;
  id: string;
  workspace: string;
  description: string;
  onDispatch: () => Promise<string>;
  onCommit?: () => void;
}) {
  return (
    <TaskRunnerDialog
      open={open}
      onOpenChange={onOpenChange}
      title={`Delete ${kind} · ${name}`}
      description={description}
      type="remove"
      target={id}
      workspace={workspace}
      destructive
      confirmText={name}
      startLabel={`Delete ${kind}`}
      onDispatch={onDispatch}
      onCommit={onCommit}
    />
  );
}
