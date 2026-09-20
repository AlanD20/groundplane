'use client'

import { Dialog as DialogPrimitive } from '@base-ui/react/dialog'
import { X } from 'lucide-react'
import { cn } from '@/lib/utils'

// Right-side drawer (sheet) for forms that need more room than a centered
// dialog — e.g. the service create/edit form. Same base-ui modal primitives
// as Dialog, with 'trap-focus' so opening/closing never locks page scroll.
function Drawer(props: DialogPrimitive.Root.Props) {
  return <DialogPrimitive.Root modal="trap-focus" {...props} />
}

function DrawerContent({
  className,
  children,
  showClose = true,
  ...props
}: DialogPrimitive.Popup.Props & { showClose?: boolean }) {
  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Backdrop className="fixed inset-0 z-50 bg-background/70 backdrop-blur-sm transition-all duration-150 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
      <DialogPrimitive.Popup
        className={cn(
          'fixed inset-y-0 right-0 z-50 flex w-full max-w-xl flex-col gap-4 overflow-y-auto border-l border-border bg-popover p-6 text-popover-foreground shadow-2xl outline-none',
          'transition-all duration-200 ease-out data-[ending-style]:translate-x-full data-[starting-style]:translate-x-full',
          className,
        )}
        {...props}
      >
        {children}
        {showClose && (
          <DialogPrimitive.Close className="absolute right-3.5 top-3.5 rounded-md p-1 text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring">
            <X className="size-4" />
            <span className="sr-only">Close</span>
          </DialogPrimitive.Close>
        )}
      </DialogPrimitive.Popup>
    </DialogPrimitive.Portal>
  )
}

export { Drawer, DrawerContent }
