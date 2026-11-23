import type { ReactNode } from "react";
import { AnimatedSection } from "@/components/common/animated-section";
import { Disclosure } from "@/components/common/disclosure";
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
  editor,
  label,
  value,
  source,
  environmentName,
  exposure,
  file,
  secret,
  encryption,
  emptySecretValue,
  loadValue,
  copyValue,
  onEdit,
  editing = false,
  onRemove,
}: {
  editor?: ReactNode;
  label: string;
  value?: string;
  source: string;
  environmentName: string;
  exposure: string[];
  file?: boolean;
  secret?: boolean;
  encryption?: "age" | "none";
  emptySecretValue?: boolean;
  loadValue?: () => Promise<string>;
  copyValue?: string;
  onEdit?: () => void;
  editing?: boolean;
  onRemove?: () => void;
}) {
  const componentOnly = exposure.includes("component:edge-tunnel");
  const exposureLabel = componentOnly ? "Cloudflare Tunnel only" : exposure.includes("all")
    ? "All Services"
    : exposure.length === 1
      ? `Only ${exposure[0]}`
      : exposure.length
        ? `Shared · ${exposure.length} Services`
        : "No Services";
  const exposureDescription = componentOnly ? "Cloudflare Tunnel only" : exposure.includes("all")
    ? "all Services"
    : exposure.join(", ") || "no Services";
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
            <Badge variant="outline">{file ? (secret ? "Secret file" : "File") : (secret ? "Secret" : "Variable")}</Badge>
            <Badge variant="outline">{(encryption ?? (secret ? "age" : "none")) === "age" ? "Encrypted" : "Unencrypted"}</Badge>
            {secret && <EmptySecretValueBadge empty={emptySecretValue} />}
          </div>
          <div className="flex flex-wrap items-center gap-1.5">
            <Badge
              variant="outline"
              title="Definition owned by this Environment"
            >
              Environment: {environmentName}
            </Badge>
            <Badge
              variant="muted"
              title={`Available to ${exposureDescription}`}
            >
              {exposureLabel}
            </Badge>
          </div>
          <p className="text-xs text-muted-foreground [overflow-wrap:anywhere]">
            {source}
          </p>
        </div>
        <div className="ml-auto flex shrink-0 items-center gap-1">
          {onEdit && (
            <Button
              variant="ghost"
              size="sm"
              onClick={onEdit}
              aria-label={`Edit ${label}`}
              aria-expanded={editing}
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
                  <Trash2 /> Remove {file ? "file" : secret ? "secret" : "variable"}
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          )}
        </div>
      </div>
      {loadValue ? (
        <RevealValue loadValue={loadValue} label={label} file={file} sensitive={secret} />
      ) : file ? (
        <Disclosure
          className="min-w-0 rounded-lg border border-border"
          summaryClassName="cursor-pointer px-3 py-2 text-sm text-muted-foreground focus-visible:outline-2 focus-visible:outline-ring"
          summary={<>Preview file contents</>}
        >
          <div className="min-w-0 border-t border-border p-3">{preview}</div>
        </Disclosure>
      ) : (
        preview
      )}
      <AnimatedSection open={!!editor}>{editor}</AnimatedSection>
    </li>
  );
}
