import { useLayoutEffect, useRef, useState, type ReactNode } from "react";

/** Keep a section anchored while its summary is replaced by a local editor. */
export function InlineEditorRegion({
  editing,
  children,
}: {
  editing: boolean;
  children: ReactNode;
}) {
  const content = useRef<HTMLDivElement>(null);
  const [height, setHeight] = useState<number>();
  useLayoutEffect(() => {
    const element = content.current;
    if (!element) return;
    const measure = () => setHeight(element.getBoundingClientRect().height);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    return () => observer.disconnect();
  }, []);
  useLayoutEffect(() => {
    if (editing)
      content.current
        ?.querySelector<HTMLElement>(
          'input, button[role="combobox"], select, textarea',
        )
        ?.focus({ preventScroll: true });
  }, [editing]);
  return (
    <div
      className="min-w-0 overflow-clip transition-[height] duration-200 ease-out motion-reduce:transition-none"
      style={{ height }}
    >
      <div ref={content} className="min-w-0 p-1">
        {children}
      </div>
    </div>
  );
}
