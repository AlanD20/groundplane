import { TableRow } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import type { ComponentProps, ReactNode } from "react";
import { useNavigate } from "react-router-dom";

export function ResourceTable({ children }: { children: ReactNode }) {
  return (
    <div className="overflow-x-auto rounded-xl border border-border bg-card">
      {children}
    </div>
  );
}

// Retain the native link/button in a cell for keyboard and new-tab navigation.
// Only a plain click on the non-interactive part of a row opens its resource.
export function ResourceRow({
  href,
  onOpen,
  className,
  ...props
}: Omit<ComponentProps<"tr">, "onClick"> & {
  href?: string;
  onOpen?: () => void;
}) {
  const navigate = useNavigate();
  return (
    <TableRow
      {...props}
      className={cn(
        "cursor-pointer hover:bg-primary/5 focus-within:bg-primary/5",
        className,
      )}
      onClick={(event) => {
        if (
          event.defaultPrevented ||
          event.button !== 0 ||
          event.metaKey ||
          event.ctrlKey ||
          event.shiftKey ||
          event.altKey
        )
          return;
        if (
          !(event.target instanceof Element) ||
          event.target.closest(
            'a,button,input,select,textarea,summary,[role="button"],[role="combobox"],[data-row-action]',
          )
        )
          return;
        if (window.getSelection()?.toString()) return;
        if (href) navigate(href);
        else onOpen?.();
      }}
    />
  );
}
