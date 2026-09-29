"use client";

import { Button } from "@/components/ui/button";

import { ActivityIcon } from "@/components/common/activity-icon";
import { StatusBadge } from "@/components/common/status-badge";
import { TaskDetailDrawer } from "@/components/common/task-detail-drawer";
import { taskPresentation } from "@/features/task/task-overview";
import { formatTimestamp } from "@/lib/format-timestamp";
import { useStore } from "@/lib/store";
import type {
  ActivityEntry,
  TaskJournalScope,
  TaskJournalSurface,
} from "@/lib/types";
import { ArrowUpRight } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";

// The row always inspects the Task. Resource navigation is a separate link,
// including an explicitly labelled journal fallback when the target is gone.
export function TaskJournalItem({
  entry,
  scope,
  surface,
}: {
  entry: ActivityEntry;
  scope: TaskJournalScope;
  surface: TaskJournalSurface;
}) {
  const store = useStore();
  const [open, setOpen] = useState(false);
  const view = taskPresentation(entry, store);
  const destination = view.destination;

  const body = (
    <>
      <ActivityIcon type={entry.type} status={entry.status} />
      <div className="flex min-w-0 flex-1 flex-col gap-1">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <span className="break-words text-xs font-medium">{view.title}</span>
        </div>
        {entry.note && (
          <p className="line-clamp-2 break-all text-xs text-muted-foreground">
            {entry.note}
          </p>
        )}
        <p className="truncate text-[11px] text-muted-foreground">
          {view.scope} ·{" "}
          {formatTimestamp(entry.createdAt ?? entry.ts, "Time unavailable")}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-3">
        <StatusBadge status={entry.status} />
      </div>
    </>
  );

  return (
    <>
      <div className="flex w-full items-stretch rounded-md transition-colors hover:bg-primary/5 focus-within:bg-primary/5">
        <Button
          variant="ghost"
          size="content"
          type="button"
          onClick={() => setOpen(true)}
          className="min-w-0 flex-1 items-start justify-start gap-3 rounded-l-lg rounded-r-none p-3.5 text-left hover:bg-surface/40"
          aria-label={`Inspect ${entry.title}, ${entry.status}, Task ${entry.id}`}
          aria-haspopup="dialog"
        >
          {body}
        </Button>
        {destination && (
          <Button
            variant="outline"
            size="sm"
            render={<Link to={destination.href} />}
            nativeButton={false}
            className="m-2 ml-0 self-center"
            aria-label={`${destination.label} for Task ${entry.id}`}
            title={destination.label}
          >
            <ArrowUpRight className="size-3.5" />
          </Button>
        )}
      </div>
      {open && (
        <TaskDetailDrawer
          entry={entry}
          scope={scope}
          surface={surface}
          onOpenChange={setOpen}
        />
      )}
    </>
  );
}
