import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Select } from "@/components/ui/select";
import { useLayoutEffect, useRef, type ReactNode } from "react";

export type InspectionItem = {
  id: string;
  label: string;
  description?: string;
  status?: ReactNode;
};

export function InspectionWorkspace({
  title,
  items,
  selectedId,
  selectionDisabled = false,
  onSelect,
  selectionLabel,
  detailsLabel,
  emptyMessage,
  toolbar,
  footer,
  actionId,
  children,
}: {
  title: string;
  items: InspectionItem[];
  selectedId?: string;
  selectionDisabled?: boolean;
  onSelect: (id: string) => void;
  selectionLabel: string;
  detailsLabel: string;
  emptyMessage: string;
  toolbar: ReactNode;
  footer?: ReactNode;
  actionId?: string;
  children: ReactNode;
}) {
  const detail = useRef<HTMLElement>(null);
  useLayoutEffect(() => {
    if (detail.current) detail.current.scrollTop = 0;
  }, [selectedId]);
  return (
    <div className="grid min-w-0 items-start gap-4 lg:h-[clamp(32rem,80dvh,56rem)] lg:grid-cols-[minmax(15rem,19rem)_minmax(0,1fr)] lg:items-stretch">
      <Card className="min-w-0 lg:flex lg:min-h-0 lg:flex-col">
        <CardHeader className="shrink-0 gap-3 px-4 py-3">
          <CardTitle>{title}</CardTitle>
          {toolbar}
        </CardHeader>
        <CardContent className="space-y-2 p-2 lg:flex lg:min-h-0 lg:flex-1 lg:flex-col">
          {items.length ? (
            <>
              <Select
                className="lg:hidden"
                aria-label={selectionLabel}
                data-action-id={actionId}
                value={selectedId ?? ""}
                disabled={selectionDisabled}
                onValueChange={onSelect}
                options={items.map(({ id, label, description }) => ({
                  value: id,
                  label: description ? `${label} · ${description}` : label,
                }))}
              />
              <ul
                aria-label={title}
                className="hidden min-h-0 flex-1 space-y-1 overflow-y-auto p-1 lg:block"
              >
                {items.map(({ id, label, description, status }) => (
                  <li key={id}>
                    <button
                      type="button"
                      data-action-id={actionId}
                      aria-pressed={id === selectedId}
                      disabled={selectionDisabled && id !== selectedId}
                      onClick={() => onSelect(id)}
                      className="flex w-full min-w-0 items-center justify-between gap-2 rounded-lg border border-transparent px-3 py-2.5 text-left text-sm hover:bg-muted/60 focus-visible:outline-2 focus-visible:outline-ring aria-pressed:border-border aria-pressed:bg-accent disabled:opacity-60"
                    >
                      <span className="min-w-0 space-y-1 [overflow-wrap:anywhere]">
                        <span className="block font-medium">{label}</span>
                        {description && (
                          <span className="block text-xs text-muted-foreground">
                            {description}
                          </span>
                        )}
                      </span>
                      {status}
                    </button>
                  </li>
                ))}
              </ul>
            </>
          ) : (
            <p
              role="status"
              className="px-2 py-3 text-sm text-muted-foreground"
            >
              {emptyMessage}
            </p>
          )}
          {footer && <div className="shrink-0 px-2 pb-1">{footer}</div>}
        </CardContent>
      </Card>
      <section
        ref={detail}
        aria-label={detailsLabel}
        tabIndex={0}
        className="h-[clamp(20rem,65dvh,42rem)] min-h-0 min-w-0 space-y-3 overflow-y-auto rounded-xl border border-border bg-card p-4 [scrollbar-gutter:stable] focus-visible:outline-2 focus-visible:outline-ring lg:h-full"
      >
        {(selectedId && children) || (
          <p className="text-sm text-muted-foreground">{emptyMessage}</p>
        )}
      </section>
    </div>
  );
}
