import { TaskRunnerDialog } from "@/components/common/task-runner-dialog";
import { Button } from "@/components/ui/button";
import { useRequiredParams } from "@/lib/router";
import { useStore } from "@/lib/store";
import type { Environment, ReleaseGroup } from "@/lib/types";
import { History, Pencil, Rocket, Trash2 } from "lucide-react";
import { useState, type ReactNode } from "react";
import { ReleaseGroupFormDrawer } from "./release-group-form-drawer";
import {
  DeployReleaseGroupDialog,
  RollbackReleaseGroupDialog,
} from "./release-group-operation-dialogs";

type Operation = "edit" | "deploy" | "rollback" | "remove";

export function ReleaseGroupControls({
  env,
  group,
  onRemoved,
  children,
}: {
  env: Environment;
  group: ReleaseGroup;
  onRemoved?: () => void;
  children: (controls: { actions: ReactNode; forms: ReactNode }) => ReactNode;
}) {
  const store = useStore();
  const { tenant } = useRequiredParams("tenant");
  const [operation, setOperation] = useState<Operation | null>(null);
  const toggle = (action: Operation) =>
    setOperation((current) => (current === action ? null : action));
  const setOpen = (action: Operation, open: boolean) =>
    setOperation((current) =>
      open ? action : current === action ? null : current,
    );

  return (
    <>
      {children({
        actions: (
          <>
            <Button
              variant="outline"
              size="sm"
              aria-expanded={operation === "edit"}
              onClick={() => toggle("edit")}
            >
              <Pencil /> Edit group
            </Button>
            <Button
              variant="outline"
              size="sm"
              aria-expanded={operation === "rollback"}
              onClick={() => toggle("rollback")}
            >
              <History /> Roll back group
            </Button>
            <Button
              variant="ghost"
              size="sm"
              aria-haspopup="dialog"
              onClick={() => toggle("remove")}
            >
              <Trash2 /> Remove group
            </Button>
            <Button
              size="sm"
              aria-expanded={operation === "deploy"}
              onClick={() => toggle("deploy")}
            >
              <Rocket /> Deploy group
            </Button>
          </>
        ),
        forms: (
          <>
            <ReleaseGroupFormDrawer
              env={env}
              group={group}
              open={operation === "edit"}
              onOpenChange={(open) => setOpen("edit", open)}
            />
            <DeployReleaseGroupDialog
              env={env}
              group={group}
              open={operation === "deploy"}
              onOpenChange={(open) => setOpen("deploy", open)}
            />
            <RollbackReleaseGroupDialog
              env={env}
              group={group}
              open={operation === "rollback"}
              onOpenChange={(open) => setOpen("rollback", open)}
            />
          </>
        ),
      })}
      <TaskRunnerDialog
        runAfter
        open={operation === "remove"}
        onOpenChange={(open) => setOpen("remove", open)}
        title={`Remove ${group.name}`}
        description="Remove this group. Member Services are kept."
        type="destroy"
        target={group.id}
        workspace={tenant}
        destructive
        startLabel="Remove group"
        onDispatch={(after) =>
          store.removeReleaseGroup(env.id, group.id, after)
        }
        onCommit={() => {
          void store
            .refreshEnvironmentReleases(env.id)
            .catch(() => undefined)
            .finally(() => onRemoved?.());
        }}
      />
    </>
  );
}
