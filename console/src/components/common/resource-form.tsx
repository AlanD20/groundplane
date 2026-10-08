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
import { AnimatedSection } from "./animated-section";
import { cn } from "@/lib/utils";

const Inline = createContext(false);

/** Form content inside an existing workspace section that owns its expansion. */
export function ResourceFormBody({ children }: { children: ReactNode }) {
  return (
    <Inline.Provider value>
      <div className="space-y-4">{children}</div>
    </Inline.Provider>
  );
}

/** One form body, with contextual inline editing or a bounded modal entry point. */
export function ResourceForm({
  open,
  onOpenChange,
  inline = true,
  editing = true,
  children,
  className,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  inline?: boolean;
  editing?: boolean;
  children: ReactNode;
  className?: string;
}) {
  const region = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open || !inline || !editing) return;
    const previousFocus = document.activeElement as HTMLElement | null;
    const frame = requestAnimationFrame(() => {
      region.current?.scrollIntoView({ block: "nearest" });
      region.current
        ?.querySelector<HTMLElement>(
          "input, button[role=combobox], select, textarea",
        )
        ?.focus({ preventScroll: true });
    });
    return () => {
      cancelAnimationFrame(frame);
      if (region.current?.contains(document.activeElement))
        previousFocus?.focus({ preventScroll: true });
    };
  }, [open, inline, editing]);
  return (
    <Inline.Provider value={inline}>
      {inline ? (
        <AnimatedSection open={open} className="w-full min-w-0 basis-full">
          <div
            ref={region}
            className={cn(
              "w-full min-w-0 basis-full scroll-mt-24 py-2",
              className,
            )}
          >
            <div className="w-full">
              <section
                className={workspaceSectionClassName(
                  editing,
                  "space-y-4 [&_input:not([type=checkbox]):not([type=radio])]:max-w-lg [&_[role=combobox]]:max-w-lg",
                )}
              >
                {children}
              </section>
            </div>
          </div>
        </AnimatedSection>
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
