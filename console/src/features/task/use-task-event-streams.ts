import { useCallback, useEffect, useRef } from "react";
import type { TaskEventResponse } from "./journal-model";
import { followTaskEvents } from "./follow-events";

export type TaskEventActions = {
  watchTaskEvents: (
    taskId: string,
    onEvent: (event: TaskEventResponse) => void,
    onNotice: (message: string) => void,
  ) => () => void;
};

export function useTaskEventStreams(): TaskEventActions & {
  closeTaskStreams: () => void;
} {
  const streams = useRef(new Set<AbortController>());
  const closeTaskStreams = useCallback(() => {
    for (const controller of streams.current) controller.abort();
    streams.current.clear();
  }, []);
  useEffect(() => closeTaskStreams, [closeTaskStreams]);
  const watchTaskEvents = useCallback<TaskEventActions["watchTaskEvents"]>(
    (taskId, onEvent, onNotice) => {
      const controller = new AbortController();
      streams.current.add(controller);
      void followTaskEvents(taskId, controller.signal, onEvent, onNotice)
        .catch((error: unknown) => {
          if (!controller.signal.aborted)
            onNotice(
              error instanceof Error
                ? error.message
                : "Task event observation stopped",
            );
        })
        .finally(() => streams.current.delete(controller));
      return () => {
        controller.abort();
        streams.current.delete(controller);
      };
    },
    [],
  );
  return { closeTaskStreams, watchTaskEvents };
}
