import { useId, useState, type ReactNode } from "react";
import { AnimatedSection } from "./animated-section";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";

/** Accessible disclosure with the same expansion as workspace forms. */
export function Disclosure({
  summary,
  children,
  className,
  summaryClassName,
  open,
  onOpenChange,
  indicator = true,
}: {
  summary: ReactNode;
  children: ReactNode;
  className?: string;
  summaryClassName?: string;
  open?: boolean;
  indicator?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const [localOpen, setLocalOpen] = useState(false);
  const expanded = open ?? localOpen;
  const id = useId();
  return (
    <div className={cn("group", className)} data-open={expanded}>
      <button
        type="button"
        aria-expanded={expanded}
        aria-controls={id}
        className={cn(
          "flex w-full items-center gap-2 cursor-pointer text-left focus-visible:outline-2 focus-visible:outline-ring",
          summaryClassName,
        )}
        onClick={() => {
          setLocalOpen(!expanded);
          onOpenChange?.(!expanded);
        }}
      >
        {indicator && (
          <ChevronRight
            aria-hidden
            className={cn(
              "size-3.5 shrink-0 transition-transform duration-200 motion-reduce:transition-none",
              expanded && "rotate-90",
            )}
          />
        )}
        {summary}
      </button>
      <div id={id}>
        <AnimatedSection open={expanded}>{children}</AnimatedSection>
      </div>
    </div>
  );
}
