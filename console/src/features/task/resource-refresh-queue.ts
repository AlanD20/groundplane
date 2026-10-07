type Refresh = (key: string, isCurrent: () => boolean) => Promise<void>;
export class RefreshSuperseded extends Error {}

// Coalesce matching invalidations and discard reads superseded during flight.
// A failed read stays retryable rather than pretending the resource is current.
export class ResourceRefreshQueue {
  private versions = new Map<string, number>();
  private pending = new Set<string>();
  private running = new Set<string>();
  private failed = new Map<string, string>();
  private timer: ReturnType<typeof setTimeout> | undefined;
  private closed = false;

  private refresh: Refresh;
  private report: (errors: string[]) => void;
  private delay: number;
  constructor(
    refresh: Refresh,
    report: (errors: string[]) => void,
    delay = 100,
  ) {
    this.refresh = refresh;
    this.report = report;
    this.delay = delay;
  }

  invalidate(key: string) {
    if (this.closed) return;
    this.versions.set(key, (this.versions.get(key) ?? 0) + 1);
    this.pending.add(key);
    this.schedule();
  }

  retry() {
    for (const key of this.failed.keys()) this.invalidate(key);
  }

  close() {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.pending.clear();
  }

  private schedule() {
    if (this.timer || this.closed) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      for (const key of this.pending) {
        if (!this.running.has(key)) void this.run(key);
      }
    }, this.delay);
  }

  private async run(key: string) {
    this.pending.delete(key);
    this.running.add(key);
    const version = this.versions.get(key);
    const isCurrent = () => !this.closed && this.versions.get(key) === version;
    try {
      await this.refresh(key, isCurrent);
      if (isCurrent()) this.failed.delete(key);
    } catch (cause) {
      if (isCurrent()) {
        if (cause instanceof RefreshSuperseded) this.invalidate(key);
        else
          this.failed.set(
            key,
            cause instanceof Error ? cause.message : "Resource refresh failed",
          );
      }
    } finally {
      this.running.delete(key);
      if (!this.closed) {
        this.report([...this.failed.values()]);
        if (this.pending.has(key)) this.schedule();
      }
    }
  }
}
