"use client";

import { cn } from "@/lib/utils";
import { Tooltip as TooltipPrimitive } from "@base-ui/react/tooltip";
import { useId, useState } from "react";

function TooltipProvider({ children }: { children: React.ReactNode }) {
  return (
    <TooltipPrimitive.Provider delay={200}>
      {children}
    </TooltipPrimitive.Provider>
  );
}

function Tooltip({
  content,
  children,
  side = "top",
  showOnClick = false,
}: {
  content: React.ReactNode;
  children: React.ReactNode;
  side?: TooltipPrimitive.Positioner.Props["side"];
  showOnClick?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const triggerId = useId();
  return (
    <TooltipPrimitive.Root
      open={showOnClick ? open : undefined}
      onOpenChange={showOnClick ? setOpen : undefined}
      triggerId={showOnClick ? triggerId : undefined}
      disableHoverablePopup={showOnClick}
    >
      <TooltipPrimitive.Trigger
        id={showOnClick ? triggerId : undefined}
        aria-describedby={
          showOnClick && open ? `${triggerId}-description` : undefined
        }
        closeOnClick={!showOnClick}
        onClick={showOnClick ? () => setOpen(true) : undefined}
        onPointerLeave={
          showOnClick
            ? (event) => {
                if (event.pointerType !== "touch") setOpen(false);
              }
            : undefined
        }
        render={children as React.ReactElement}
      />
      <TooltipPrimitive.Portal>
        <TooltipPrimitive.Positioner
          side={side}
          sideOffset={6}
          className="z-50"
        >
          <TooltipPrimitive.Popup
            id={showOnClick ? `${triggerId}-description` : undefined}
            role={showOnClick ? "tooltip" : undefined}
            className={cn(
              "max-w-80 rounded-md border border-border bg-popover px-3 py-2 text-xs leading-relaxed text-popover-foreground shadow-md",
              "transition-all duration-100 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0",
            )}
          >
            {content}
          </TooltipPrimitive.Popup>
        </TooltipPrimitive.Positioner>
      </TooltipPrimitive.Portal>
    </TooltipPrimitive.Root>
  );
}

export { Tooltip, TooltipProvider };
