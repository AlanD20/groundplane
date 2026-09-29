import type { Environment, TaskJournalScope } from "@/lib/types";
import { useMemo } from "react";
import { TaskList } from "./task-list";

export function TasksCard({ env }: { env: Environment }) {
  const scope = useMemo<TaskJournalScope>(
    () => ({ kind: "environment", environmentId: env.id }),
    [env.id],
  );
  return <TaskList scope={scope} title="Environment Tasks" />;
}
