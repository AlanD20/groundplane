"use client";

import { Pencil, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { RevealValue } from "@/components/common/reveal-value";

export function EntryRow({
  label,
  value,
  path,
  file,
  secret,
  loadValue,
  onEdit,
  onRemove,
}: {
  label: string;
  value?: string;
  path?: string;
  file?: boolean;
  secret?: boolean;
  loadValue?: () => Promise<string>;
  onEdit?: () => void;
  onRemove?: () => void;
}) {
  return (
    <div className="flex items-center justify-between rounded-lg border border-border bg-surface px-3 py-2">
      <span className="flex min-w-0 items-center gap-2">
        <span className="truncate font-mono text-sm">{label}</span>
        {file && <Badge variant="muted">file</Badge>}
        {secret && <Badge variant="warning">secret</Badge>}
      </span>
      <span className="flex items-center gap-2">
        {path && (
          <span className="truncate font-mono text-xs text-muted-foreground">
            {path}
          </span>
        )}
        {secret && loadValue ? (
          <RevealValue loadValue={loadValue} label={label} />
        ) : !path ? (
          <span className="truncate font-mono text-xs text-muted-foreground">
            {value}
          </span>
        ) : null}
        {onEdit && (
          <Button
            variant="ghost"
            size="icon-sm"
            className="text-muted-foreground hover:text-primary"
            onClick={onEdit}
            title={`Edit ${label}`}
          >
            <Pencil className="size-3.5" />
          </Button>
        )}
        {onRemove && (
          <Button
            variant="ghost"
            size="icon-sm"
            className="text-muted-foreground hover:text-destructive"
            onClick={onRemove}
            title={`Remove ${label}`}
          >
            <Trash2 className="size-3.5" />
          </Button>
        )}
      </span>
    </div>
  );
}
