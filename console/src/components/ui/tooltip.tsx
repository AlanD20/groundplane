'use client'

import { Tooltip as TooltipPrimitive } from '@base-ui/react/tooltip'
import { cn } from '@/lib/utils'

function TooltipProvider({ children }: { children: React.ReactNode }) {
  return <TooltipPrimitive.Provider delay={200}>{children}</TooltipPrimitive.Provider>
}

function Tooltip({
  content,
  children,
  side = 'top',
}: {
  content: React.ReactNode
  children: React.ReactNode
  side?: TooltipPrimitive.Positioner.Props['side']
}) {
  return (
    <TooltipPrimitive.Root>
      <TooltipPrimitive.Trigger render={children as React.ReactElement} />
      <TooltipPrimitive.Portal>
        <TooltipPrimitive.Positioner side={side} sideOffset={6} className="z-50">
          <TooltipPrimitive.Popup
            className={cn(
              'rounded-md border border-border bg-popover px-2 py-1 text-xs text-popover-foreground shadow-md',
              'transition-all duration-100 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0',
            )}
          >
            {content}
          </TooltipPrimitive.Popup>
        </TooltipPrimitive.Positioner>
      </TooltipPrimitive.Portal>
    </TooltipPrimitive.Root>
  )
}

export { Tooltip, TooltipProvider }
