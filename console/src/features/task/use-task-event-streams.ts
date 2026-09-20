import { useCallback, useRef } from "react";
import { parseTaskEvent, type TaskEventResponse } from "./journal-model";
export type TaskEventActions = {
  watchTaskEvents: (
    taskId: string,
    onEvent: (event: TaskEventResponse) => void,
    onMalformed: (message: string) => void,
  ) => () => void;
};
export function useTaskEventStreams(): TaskEventActions & {
  closeTaskStreams: () => void;
} {
  const taskEventSources = useRef(new Set<EventSource>());
  const closeTaskStreams = useCallback(() => {
    for (const source of taskEventSources.current) source.close();
    taskEventSources.current.clear();
  }, []);
  return {
    closeTaskStreams,
    watchTaskEvents: (taskId, onEvent, onMalformed) => {
      const source = new EventSource(
        `/api/v1/tasks/${encodeURIComponent(taskId)}/events`,
      );
      taskEventSources.current.add(source);
      const close = () => {
        source.close();
        taskEventSources.current.delete(source);
      };
      source.onmessage = (message) => {
        try {
          onEvent(parseTaskEvent(message.data));
        } catch {
          close();
          onMalformed("Task event stream returned malformed data");
        }
      };
      return close;
    },
  };
}
