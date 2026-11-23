const eventRefreshDelay = 200;

// Events invalidate the snapshot, including when a read is already in flight.
// Keep one read at a time and coalesce bursts into one follow-up read.
export function createTaskDetailRefresh(
  read: () => Promise<void>,
  fallbackInterval?: number,
) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let reading = false;
  let invalidated = false;
  let stopped = false;

  function schedule(delay: number) {
    clearTimeout(timer);
    timer = setTimeout(() => void refresh(), delay);
  }

  async function refresh() {
    if (stopped || reading) return;
    clearTimeout(timer);
    invalidated = false;
    reading = true;
    try {
      await read();
    } finally {
      reading = false;
      if (!stopped) {
        if (invalidated) schedule(eventRefreshDelay);
        else if (fallbackInterval !== undefined) schedule(fallbackInterval);
      }
    }
  }

  return {
    start: refresh,
    invalidate() {
      if (stopped || invalidated) return;
      invalidated = true;
      if (!reading) schedule(eventRefreshDelay);
    },
    stop() {
      stopped = true;
      clearTimeout(timer);
    },
  };
}
