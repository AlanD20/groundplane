import { AnimatedSection } from "./animated-section";
import { useLayoutEffect, useRef, type ReactNode } from "react";

/** Keep a section anchored while its summary is replaced by a local editor. */
export function InlineEditorRegion({
  editing,
  children,
}: {
  editing: boolean;
  children: ReactNode;
}) {
  const content = useRef<HTMLDivElement>(null);
  useLayoutEffect(() => {
    if (editing)
      content.current
        ?.querySelector<HTMLElement>(
          'input, button[role="combobox"], select, textarea',
        )
        ?.focus({ preventScroll: true });
  }, [editing]);
  return (
    <AnimatedSection>
      <div ref={content} className="min-w-0 p-1">
        {children}
      </div>
    </AnimatedSection>
  );
}
