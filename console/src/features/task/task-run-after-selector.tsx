import { useEffect, useId, useState } from "react";
import { SearchableChecklist } from "@/components/common/searchable-checklist";
import { TaskLink } from "@/components/common/task-link";
import { StatusBadge } from "@/components/common/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useStore } from "@/lib/store";
import type { ActivityEntry } from "@/lib/types";

/** Whole-Task prerequisites; the Controller owns ordering and failure propagation. */
export function TaskRunAfterSelector({
  value,
  onChange,
  onBusyChange,
}: {
  value: string[];
  onChange: (ids: string[]) => void;
  onBusyChange: (busy: boolean) => void;
}) {
  const { getTaskJournal, loadTaskJournal, getTaskJournalDetail } = useStore();
  const journal = getTaskJournal({ kind: "all" });
  const [known, setKnown] = useState<Record<string, ActivityEntry>>({});
  const [manual, setManual] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const inputId = useId();
  useEffect(() => {
    onBusyChange(loading || manual.trim().length > 0);
  }, [loading, manual, onBusyChange]);
  useEffect(() => {
    if (!journal.loaded && !journal.loading && !journal.loadError)
      void loadTaskJournal("tasks", { kind: "all" }).catch(() => undefined);
  }, [journal.loaded, journal.loading, journal.loadError, loadTaskJournal]);
  const tasks = new Map(
    [...Object.values(known), ...journal.entries].map((task) => [
      task.id,
      task,
    ]),
  );
  async function add() {
    const id = manual.trim();
    if (!/^task_[0-9A-HJKMNP-TV-Z]{26}$/.test(id)) {
      setError("Enter a valid Task ID.");
      return;
    }
    if (value.includes(id)) {
      setManual("");
      return;
    }
    if (value.length >= 16) {
      setError("Choose at most 16 Tasks.");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const task = await getTaskJournalDetail(id);
      setKnown((current) => ({ ...current, [id]: task }));
      onChange([...value, id]);
      setManual("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to load Task");
    } finally {
      setLoading(false);
    }
  }
  return (
    <section
      className="min-w-0 space-y-3 rounded-lg border border-border p-3"
      aria-label="Whole-Task prerequisites"
    >
      <fieldset
        disabled={loading}
        data-form-shortcuts="ignore"
        onKeyDown={(event) => {
          if (
            event.key === "Enter" &&
            event.target instanceof HTMLInputElement &&
            event.target.type !== "checkbox"
          ) {
            event.preventDefault();
            event.stopPropagation();
          }
        }}
      >
        <SearchableChecklist
          label="Run after · whole Tasks"
          maximum={16}
          selected={value}
          onChange={onChange}
          options={[...tasks.values()].map((task) => ({
            id: task.id,
            label: task.title,
            detail: `${task.status.replaceAll("_", " ")} · ${task.id}`,
          }))}
        />
      </fieldset>
      <p className="text-xs text-muted-foreground">
        Optional. Preparation starts only after every selected Task succeeds and
        fully settles. A failed, cancelled or superseded prerequisite prevents
        this action from running. This selects whole Tasks, not their individual
        operations.
      </p>
      <div
        data-form-shortcuts="ignore"
        className="space-y-2"
        onKeyDown={(event) => {
          if (
            event.key === "Enter" &&
            event.target instanceof HTMLInputElement
          ) {
            event.preventDefault();
            event.stopPropagation();
            if (!loading) void add();
          }
        }}
      >
        <Label htmlFor={inputId}>Add another Task by ID</Label>
        <div className="flex gap-2">
          <Input
            id={inputId}
            value={manual}
            onChange={(event) => setManual(event.target.value)}
            placeholder="task_…"
            disabled={loading}
          />
          <Button
            type="button"
            variant="outline"
            disabled={loading || !manual.trim() || value.length >= 16}
            onClick={() => void add()}
          >
            {loading ? "Loading…" : "Add Task"}
          </Button>
        </div>
      </div>
      {journal.loading && (
        <p role="status" className="text-xs text-muted-foreground">
          Loading recent Tasks…
        </p>
      )}
      {journal.loadError && (
        <p role="alert" className="text-xs text-destructive">
          {journal.loadError}
        </p>
      )}
      {error && (
        <p role="alert" className="text-xs text-destructive">
          {error}
        </p>
      )}
      {value.length > 0 && (
        <ul className="space-y-2">
          {value.map((id) => (
            <li key={id} className="flex flex-wrap items-center gap-2">
              <TaskLink taskId={id}>{tasks.get(id)?.title ?? id}</TaskLink>
              {tasks.has(id) && <StatusBadge status={tasks.get(id)!.status} />}
              <Button
                type="button"
                variant="ghost"
                size="sm"
                aria-label={`Remove prerequisite ${id}`}
                onClick={() =>
                  onChange(value.filter((selected) => selected !== id))
                }
              >
                Remove
              </Button>
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
