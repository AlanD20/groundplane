import {
  createContext,
  useContext,
  useId,
  useState,
  type ReactNode,
} from "react";
import { GripVertical } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

const ReorderContext = createContext<{
  id: string;
  dragging: number | null;
  setDragging: (index: number | null) => void;
  target: number | null;
  setTarget: (index: number | null) => void;
  move: (from: number, to: number) => void;
  disabled: boolean;
} | null>(null);

/** Drag handles supplement each feature's existing keyboard move buttons. */
export function ReorderList({
  children,
  onMove,
  disabled = false,
  className,
}: {
  children: ReactNode;
  onMove: (from: number, to: number) => void;
  disabled?: boolean;
  className?: string;
}) {
  const id = useId();
  const [dragging, setDragging] = useState<number | null>(null);
  const [target, setTarget] = useState<number | null>(null);
  const [announcement, setAnnouncement] = useState("");
  return (
    <ReorderContext.Provider
      value={{
        id,
        dragging,
        setDragging,
        target,
        setTarget,
        disabled,
        move(from, to) {
          if (!disabled && from !== to) {
            onMove(from, to);
            setAnnouncement(`Moved item ${from + 1} to position ${to + 1}.`);
          }
          setDragging(null);
          setTarget(null);
        },
      }}
    >
      <div className={className}>{children}</div>
      <span role="status" className="sr-only">
        {announcement}
      </span>
    </ReorderContext.Provider>
  );
}

export function ReorderItem({
  index,
  label,
  children,
  className,
}: {
  index: number;
  label: string;
  children: ReactNode;
  className?: string;
}) {
  const context = useContext(ReorderContext);
  if (!context) throw new Error("ReorderItem requires ReorderList");
  return (
    <div
      data-reorder-item
      className={cn(
        className,
        "transition-[background-color,box-shadow,opacity] duration-200 motion-reduce:transition-none",
        context.dragging === index && "opacity-50",
        context.target === index &&
          context.dragging !== index &&
          "bg-primary/10 ring-2 ring-primary",
      )}
      onDragOver={(event) => {
        if (context.disabled || context.dragging === null) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = "move";
        context.setTarget(index);
      }}
      onDrop={(event) => {
        if (
          context.disabled ||
          context.dragging === null ||
          event.dataTransfer.getData("application/x-gp-reorder") !== context.id
        )
          return;
        event.preventDefault();
        context.move(context.dragging, index);
      }}
    >
      <Button
        type="button"
        variant="ghost"
        size="icon-xs"
        aria-label={`Drag to reorder ${label}. Or use the move up and down buttons.`}
        title="Drag to reorder"
        className="shrink-0 cursor-grab active:cursor-grabbing"
        disabled={context.disabled}
        draggable={!context.disabled}
        onDragStart={(event) => {
          event.dataTransfer.effectAllowed = "move";
          event.dataTransfer.setData("application/x-gp-reorder", context.id);
          const row = event.currentTarget.closest<HTMLElement>(
            "[data-reorder-item]",
          );
          if (row)
            event.dataTransfer.setDragImage(row, 16, row.offsetHeight / 2);
          context.setDragging(index);
        }}
        onDragEnd={() => {
          context.setDragging(null);
          context.setTarget(null);
        }}
      >
        <GripVertical className="size-4" />
      </Button>
      {children}
    </div>
  );
}

export function reorderItems<T>(items: T[], from: number, to: number): T[] {
  if (
    from === to ||
    from < 0 ||
    to < 0 ||
    from >= items.length ||
    to >= items.length
  )
    return items;
  const next = [...items];
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
}
