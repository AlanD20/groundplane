'use client'

import { Dialog as DialogPrimitive } from '@base-ui/react/dialog'
import { X } from 'lucide-react'
import { cn } from '@/lib/utils'

const Dialog = DialogPrimitive.Root
const DialogTrigger = DialogPrimitive.Trigger
const DialogClose = DialogPrimitive.Close

function DialogContent({
  className,
  children,
  showClose = true,
  submitOnEnter = true,
  onKeyDown,
  ...props
}: DialogPrimitive.Popup.Props & { showClose?: boolean; submitOnEnter?: boolean }) {
  const handleKeyDown: NonNullable<DialogPrimitive.Popup.Props['onKeyDown']> = (event) => {
    onKeyDown?.(event)
    if (
      event.defaultPrevented ||
      !submitOnEnter ||
      event.key !== 'Enter' ||
      event.altKey ||
      event.ctrlKey ||
      event.metaKey ||
      event.shiftKey ||
      event.nativeEvent.isComposing
    ) {
      return
    }

    const target = event.target
    if (!(target instanceof HTMLInputElement || target instanceof HTMLSelectElement)) return
    if (
      target instanceof HTMLInputElement &&
      !['email', 'number', 'password', 'search', 'tel', 'text', 'url'].includes(target.type)
    ) {
      return
    }
    if (target.getAttribute('role') === 'combobox' && target.getAttribute('aria-expanded') === 'true') return

    // Native forms already provide the correct submit semantics and validation.
    const nativeForm = target.closest('form')
    if (nativeForm && event.currentTarget.contains(nativeForm)) return

    const footer = event.currentTarget.querySelector<HTMLElement>('[data-slot="dialog-footer"]')
    const actions = footer?.querySelectorAll<HTMLButtonElement>('button')
    const primaryAction = actions?.item((actions?.length ?? 0) - 1)
    if (!primaryAction || primaryAction.disabled || primaryAction.getAttribute('aria-disabled') === 'true') return

    event.preventDefault()
    primaryAction.click()
  }

  return (
    <DialogPrimitive.Portal>
      <DialogPrimitive.Backdrop className="fixed inset-0 z-50 bg-background/70 backdrop-blur-sm transition-all duration-200 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0" />
      <DialogPrimitive.Popup
        data-slot="dialog-content"
        className={cn(
          'fixed left-1/2 top-1/2 z-50 grid w-[calc(100vw-2rem)] max-h-[90dvh] overflow-y-auto max-w-lg -translate-x-1/2 -translate-y-1/2 gap-4 rounded-xl border border-border bg-popover p-6 text-popover-foreground shadow-2xl outline-none',
          'transition-all duration-150 data-[ending-style]:scale-95 data-[ending-style]:opacity-0 data-[starting-style]:scale-95 data-[starting-style]:opacity-0',
          className,
        )}
        onKeyDown={handleKeyDown}
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

function DialogHeader({ className, ...props }: React.ComponentProps<'div'>) {
  return <div className={cn('flex flex-col gap-1.5 pr-6', className)} {...props} />
}

function DialogFooter({ className, ...props }: React.ComponentProps<'div'>) {
  return (
    <div
      data-slot="dialog-footer"
      className={cn('flex flex-col-reverse gap-2 sm:flex-row sm:justify-end', className)}
      {...props}
    />
  )
}

function DialogTitle({ className, ...props }: DialogPrimitive.Title.Props) {
  return <DialogPrimitive.Title className={cn('text-base font-semibold', className)} {...props} />
}

function DialogDescription({ className, ...props }: DialogPrimitive.Description.Props) {
  return <DialogPrimitive.Description className={cn('text-sm text-muted-foreground', className)} {...props} />
}

export {
  Dialog,
  DialogTrigger,
  DialogClose,
  DialogContent,
  DialogHeader,
  DialogFooter,
  DialogTitle,
  DialogDescription,
}
