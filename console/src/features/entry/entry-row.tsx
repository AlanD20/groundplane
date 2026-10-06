"use client";

import { Pencil, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { RevealValue } from "@/components/common/reveal-value";
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
            className="max-w-72 justify-start truncate p-0 font-mono text-[11px]"
            onClick={onEdit}
            title={label}
          >
            {label}
          </Button>
          {file && <Badge variant="muted">file</Badge>}
          {secret && <Badge variant="warning">secret</Badge>}
          {secret && emptySecretValue && (
            <Badge variant="warning">empty value</Badge>
          )}
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
          <span
            title={value}
            className="block max-w-72 truncate font-mono text-xs text-muted-foreground"
          >
            {value}
          </span>
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
