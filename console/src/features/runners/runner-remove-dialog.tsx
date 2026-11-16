import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { useStore } from "@/lib/store";
import type { Runner } from "@/lib/types";

export function RunnerRemoveDialog({
  runner,
  tenantId,
  workspace,
  onClose,
}: {
  runner: Runner | null;
  tenantId: string;
  workspace: string;
  onClose: () => void;
}) {
  const store = useStore();
  return (
    <TaskRunnerDialog
      open={runner !== null}
      onOpenChange={(open) => {
        if (!open) onClose();
      }}
      title={`Remove Runner · ${runner?.slug ?? ""}`}
      description="Remove this Runner. You must also deregister it in GitHub."
      type="remove"
      target={runner?.id ?? tenantId}
      workspace={workspace}
      destructive
      startLabel="Remove Runner"
      runAfter
      onDispatch={(after) =>
        runner
          ? store.removeRunner(runner.id, after)
          : Promise.reject(new Error("No Runner selected"))
      }
      onCommit={() => {
        onClose();
        void store.refreshRunners(tenantId).catch(() => undefined);
      }}
    />
  );
}
