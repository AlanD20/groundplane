import { workspaceSectionClassName } from "@/components/common/workspace-section";
("use client");

import { File, MoreHorizontal, Pencil, Trash2 } from "lucide-react";
import { Button, buttonVariants } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { RevealValue } from "@/components/common/reveal-value";
import { CopyButton } from "@/components/common/copy-button";
import { EmptySecretValueBadge } from "@/components/common/empty-secret-value-badge";
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
} from "@/components/ui/dropdown-menu";

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
  editing = false,
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
  editing?: boolean;
  onRemove?: () => void;
}) {
  const preview = (
    <div className="flex min-w-0 items-start gap-2">
      <code className="block min-h-9 min-w-0 max-h-40 flex-1 overflow-auto whitespace-pre-wrap rounded-md bg-muted/60 px-3 py-2 font-mono text-sm [overflow-wrap:anywhere]">
        {value === "" ? (
          <span className="font-sans italic text-muted-foreground">
            Empty value
          </span>
        ) : (
          value
        )}
      </code>
      {copyValue !== undefined && (
        <CopyButton
          value={copyValue}
          label="Copy value"
          iconOnly
          className="shrink-0"
        />
      )}
    </div>
  );
  return (
    <li className={workspaceSectionClassName(editing, "space-y-3")}>
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3 sm:flex-nowrap">
        <div className="min-w-0 basis-full space-y-1.5 sm:basis-auto sm:flex-1">
          <div className="flex min-w-0 flex-wrap items-center gap-1.5">
            {file && (
              <File
                aria-hidden
                className="size-4 shrink-0 text-muted-foreground"
              />
            )}
            <span className="min-w-0 break-all font-mono text-sm font-medium">
              {label}
            </span>
            <CopyButton
              value={label}
              label={file ? "Copy path" : "Copy key"}
              iconOnly
              className="shrink-0"
            />
            {editing && <Badge variant="warning">Editing</Badge>}
            {secret && <Badge variant="muted">Encrypted</Badge>}
            {secret && <EmptySecretValueBadge empty={emptySecretValue} />}
          </div>
          <p className="text-xs text-muted-foreground [overflow-wrap:anywhere]">
            {source} · Available to {exposure}
          </p>
        </div>
        <div className="ml-auto flex shrink-0 items-center gap-1">
          {onEdit && (
            <Button
              variant="ghost"
              size="sm"
              onClick={onEdit}
              aria-label={`Edit ${label}`}
            >
              <Pencil className="size-3.5" /> Edit
            </Button>
          )}
          {onRemove && (
            <DropdownMenu>
              <DropdownMenuTrigger
                aria-label={`More actions for ${label}`}
                className={buttonVariants({
                  variant: "ghost",
                  size: "icon-sm",
                })}
              >
                <MoreHorizontal className="size-4" />
              </DropdownMenuTrigger>
              <DropdownMenuContent>
                <DropdownMenuItem variant="destructive" onClick={onRemove}>
                  <Trash2 /> Remove {file ? "file" : "variable"}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </div>
      </div>
      {secret && loadValue ? (
        <RevealValue loadValue={loadValue} label={label} />
      ) : file ? (
        <details className="min-w-0 rounded-lg border border-border">
          <summary className="cursor-pointer px-3 py-2 text-sm text-muted-foreground focus-visible:outline-2 focus-visible:outline-ring">
            Preview file contents
          </summary>
          <div className="min-w-0 border-t border-border p-3">{preview}</div>
        </details>
      ) : (
        preview
      )}
    </li>
  );
}
