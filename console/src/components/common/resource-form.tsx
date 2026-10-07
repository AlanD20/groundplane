import {
  createContext,
  useContext,
  useEffect,
  useRef,
  type ReactNode,
} from "react";
import { Drawer, DrawerContent } from "@/components/ui/drawer";
import {
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  workspaceSectionClassName,
  editorFooterClassName,
} from "./workspace-section";
import { InlineEditorRegion } from "./inline-editor-region";
import { cn } from "@/lib/utils";

const Inline = createContext(false);

/** One form body, with contextual inline editing or a bounded modal entry point. */
export function ResourceForm({
  open,
  onOpenChange,
  inline = true,
  editing = true,
  children,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  inline?: boolean;
  editing?: boolean;
  children: ReactNode;
}) {
  const region = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open || !inline) return;
    const frame = requestAnimationFrame(() =>
      region.current?.scrollIntoView({ block: "start" }),
    );
    return () => cancelAnimationFrame(frame);
  }, [open, inline]);
  if (inline && !open) return null;
  return (
    <Inline.Provider value={inline}>
      {inline ? (
        <div
          ref={region}
          className="order-last w-full min-w-0 basis-full scroll-mt-24 py-2"
        >
          <InlineEditorRegion editing={editing}>
            <section
              className={workspaceSectionClassName(editing, "space-y-4")}
            >
              {children}
            </section>
          </InlineEditorRegion>
        </div>
      ) : (
        <Drawer open={open} onOpenChange={onOpenChange}>
          <DrawerContent>{children}</DrawerContent>
        </Drawer>
      )}
    </Inline.Provider>
  );
}

export function ResourceFormHeader({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return useContext(Inline) ? (
    <div className={cn("space-y-2 border-b border-primary/25 pb-4", className)}>
      {children}
    </div>
  ) : (
    <DialogHeader className={className}>{children}</DialogHeader>
  );
}

export function ResourceFormTitle({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return useContext(Inline) ? (
    <h3 className={cn("text-sm font-semibold", className)}>{children}</h3>
  ) : (
    <DialogTitle className={className}>{children}</DialogTitle>
  );
}

export function ResourceFormFooter({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return useContext(Inline) ? (
    <div
      className={cn(
        editorFooterClassName,
        "flex flex-wrap items-center justify-end gap-2",
        className,
      )}
    >
      {children}
    </div>
  ) : (
    <DialogFooter className={className}>{children}</DialogFooter>
  );
}

export function ResourceFormDescription({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return useContext(Inline) ? (
    <p className={cn("text-xs text-muted-foreground", className)}>{children}</p>
  ) : (
    <DialogDescription className={className}>{children}</DialogDescription>
  );
}
