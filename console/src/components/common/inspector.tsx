import {
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import { useRef, type ReactNode } from "react";

export function Inspector({
  open,
  onOpenChange,
  title,
  context,
  status,
  actions,
  footer,
  children,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: ReactNode;
  context?: ReactNode;
  status?: ReactNode;
  actions?: ReactNode;
  footer?: ReactNode;
  children: ReactNode;
}) {
  const heading = useRef<HTMLHeadingElement>(null);
  return (
    <Drawer open={open} onOpenChange={onOpenChange}>
      <DrawerContent initialFocus={heading}>
        <DialogHeader>
          <div className="break-words text-[11px] text-muted-foreground">
            {context}
          </div>
          <DialogTitle
            ref={heading}
            tabIndex={-1}
            className="break-words pr-6 text-2xl font-medium tracking-tight outline-none [overflow-wrap:anywhere]"
          >
            {title}
          </DialogTitle>
          {status}
        </DialogHeader>
        {actions && (
          <div className="flex flex-wrap items-center gap-2">{actions}</div>
        )}
        <div className="min-w-0 flex-1 space-y-5">{children}</div>
        {footer && (
          <DialogFooter className="sm:justify-between">{footer}</DialogFooter>
        )}
      </DrawerContent>
    </Drawer>
  );
}
