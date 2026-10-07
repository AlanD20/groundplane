import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tooltip } from "@/components/ui/tooltip";
import { Button } from "@/components/ui/button";
import { CircleHelp } from "lucide-react";
import type { ReactNode } from "react";

export function HelpHint({
  children,
  label = "More information",
}: {
  children: ReactNode;
  label?: string;
}) {
  return (
    <Tooltip content={children} showOnClick>
      <Button
        type="button"
        variant="ghost"
        size="icon-xs"
        aria-label={label}
        className="inline-flex shrink-0 items-center rounded text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
      >
        <CircleHelp aria-hidden className="size-3.5" />
      </Button>
    </Tooltip>
  );
}

export function ResourcePanel({
  title,
  actions,
  children,
}: {
  title: ReactNode;
  actions?: ReactNode;
  children?: ReactNode;
}) {
  return (
    <Card>
      <CardHeader className="flex-row flex-wrap items-center justify-between gap-3 border-b border-border">
        <CardTitle>
          <h2>{title}</h2>
        </CardTitle>
        {actions && (
          <div className="ml-auto flex flex-wrap items-center justify-end gap-2">
            {actions}
          </div>
        )}
      </CardHeader>
      {children && (
        <CardContent className="space-y-4 pt-4">{children}</CardContent>
      )}
    </Card>
  );
}

export function SummaryStrip({ children }: { children: ReactNode }) {
  return <dl className="grid grid-cols-2 gap-3 xl:grid-cols-4">{children}</dl>;
}

export function SummaryItem({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="min-w-0 rounded-xl border border-border bg-card p-5">
      <dt className="mb-1 text-xs text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-words text-lg font-medium tracking-tight tabular-nums">
        {children}
      </dd>
    </div>
  );
}

export function AdvancedDetails({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  return (
    <details className="rounded-lg border border-border bg-card">
      <summary className="cursor-pointer rounded-lg px-4 py-3 text-xs font-medium text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring">
        {title}
      </summary>
      <div className="space-y-3 border-t border-border p-4 text-xs">
        {children}
      </div>
    </details>
  );
}

export function SettingsRow({
  title,
  help,
  children,
}: {
  title: string;
  help?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border py-4 first:pt-0 last:border-0 last:pb-0">
      <div className="flex items-center gap-2 text-sm font-medium">
        {title}
        {help && <HelpHint label={`About ${title}`}>{help}</HelpHint>}
      </div>
      <div className="flex min-w-0 items-center gap-2">{children}</div>
    </div>
  );
}
