import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { useStore } from "@/lib/store";
import type { EnvironmentEntry } from "@/lib/types";

export function EntryRemoveDialog({
  entry,
  environmentId,
  workspace,
  onClose,
}: {
  entry: EnvironmentEntry | null;
  environmentId: string;
  workspace: string;
  onClose: () => void;
}) {
  const store = useStore();
  return (
    <TaskRunnerDialog
      runAfter
      open={!!entry}
      onOpenChange={(next) => !next && onClose()}
      title={`Remove Entry · ${entry ? (entry.type === "env" ? entry.key : entry.path) : ""}`}
      description="Remove this variable or file from all exposed Services."
      type="destroy"
      target={entry?.id ?? environmentId}
      workspace={workspace}
      destructive
      confirmText={
        entry
          ? entry.type === "env"
            ? (entry.key ?? entry.id)
            : (entry.path ?? entry.id)
          : ""
      }
      startLabel="Remove Entry"
      onDispatch={async (after) => {
        if (!entry) throw new Error("No Entry selected for removal");
        return store.removeEntry(environmentId, entry.id, after);
      }}
    />
  );
}
