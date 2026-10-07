"use client";

import { Pencil, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { RevealValue } from "@/components/common/reveal-value";
import { CopyButton } from "@/components/common/copy-button";
import { EmptySecretValueBadge } from "@/components/common/empty-secret-value-badge";
import { ResourceRow } from "@/components/common/resource-table";
import { TableCell } from "@/components/ui/table";

export function EntryRow({
  label,
  value,
  source,
  exposure,
  file,
  secret,
  emptySecretValue,
  loadValue,
  copyValue,
  onEdit,
  onRemove,
}: {
  label: string;
  value?: string;
  source: string;
  exposure: string;
  file?: boolean;
  secret?: boolean;
  emptySecretValue?: boolean;
  loadValue?: () => Promise<string>;
  copyValue?: string;
  onEdit?: () => void;
  onRemove?: () => void;
}) {
  return (
    <ResourceRow onOpen={onEdit}>
      <TableCell>
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <Button
            variant="ghost"
            size="content"
            className="max-w-72 justify-start truncate p-0 font-mono text-sm"
            onClick={onEdit}
            title={label}
          >
            {label}
          </Button>
          <CopyButton value={label} label="Copy key or path" />
          {file && <Badge variant="muted">file</Badge>}
          {secret && <Badge variant="warning">secret</Badge>}
          {secret && <EmptySecretValueBadge empty={emptySecretValue} />}
        </div>
      </TableCell>
      <TableCell className="text-muted-foreground">{source}</TableCell>
      <TableCell className="max-w-96">
        {secret && loadValue ? (
          <RevealValue
            loadValue={loadValue}
            label={label}
            className="min-w-0 [&_code]:min-w-0"
          />
        ) : (
          <div className="flex min-w-0 items-center gap-2">
            <span
              title={value}
              className="block min-h-9 min-w-0 max-w-72 flex-1 truncate rounded-md bg-muted px-3 py-2 font-mono text-sm"
            >
              {value === "" ? (
                <span className="font-sans italic text-muted-foreground">
                  Empty value
                </span>
              ) : (
                value
              )}
            </span>
            {copyValue !== undefined && (
              <CopyButton value={copyValue} label="Copy value" />
            )}
          </div>
        )}
      </TableCell>
      <TableCell className="max-w-64 break-words text-muted-foreground">
        {exposure}
      </TableCell>
      <TableCell>
        <div className="flex items-center gap-2">
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
        </div>
      </TableCell>
    </ResourceRow>
  );
}
