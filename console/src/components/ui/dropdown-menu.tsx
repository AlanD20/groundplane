'use client'

import { Menu as MenuPrimitive } from '@base-ui/react/menu'
import { cn } from '@/lib/utils'

const DropdownMenu = MenuPrimitive.Root
const DropdownMenuTrigger = MenuPrimitive.Trigger
const DropdownMenuGroup = MenuPrimitive.Group

function DropdownMenuContent({
  className,
  align = 'end',
  sideOffset = 6,
  ...props
}: MenuPrimitive.Popup.Props & { align?: MenuPrimitive.Positioner.Props['align']; sideOffset?: number }) {
  return (
    <MenuPrimitive.Portal>
      <MenuPrimitive.Positioner align={align} sideOffset={sideOffset} className="z-50 outline-none">
        <MenuPrimitive.Popup
          className={cn(
            'min-w-40 origin-[var(--transform-origin)] rounded-lg border border-border bg-popover p-1 text-popover-foreground shadow-xl outline-none',
            'transition-all duration-100 data-[ending-style]:scale-95 data-[ending-style]:opacity-0 data-[starting-style]:scale-95 data-[starting-style]:opacity-0',
            className,
          )}
          {...props}
        />
      </MenuPrimitive.Positioner>
    </MenuPrimitive.Portal>
  )
}

function DropdownMenuItem({
  className,
  variant = 'default',
  ...props
}: MenuPrimitive.Item.Props & { variant?: 'default' | 'destructive' }) {
  return (
    <MenuPrimitive.Item
      className={cn(
        'flex cursor-default items-center gap-2 rounded-md px-2 py-1.5 text-sm outline-none select-none',
        'data-[highlighted]:bg-muted data-[highlighted]:text-foreground',
        variant === 'destructive' && 'text-destructive data-[highlighted]:bg-destructive/10 data-[highlighted]:text-destructive',
        '[&_svg]:size-4 [&_svg]:shrink-0 [&_svg]:text-muted-foreground data-[highlighted]:[&_svg]:text-current',
        className,
      )}
      {...props}
    />
  )
}

function DropdownMenuLabel({ className, ...props }: React.ComponentProps<'div'>) {
  return <div className={cn('px-2 py-1.5 text-xs font-medium text-muted-foreground', className)} {...props} />
}

function DropdownMenuSeparator({ className, ...props }: MenuPrimitive.Separator.Props) {
  return <MenuPrimitive.Separator className={cn('-mx-1 my-1 h-px bg-border', className)} {...props} />
}

export {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuGroup,
  DropdownMenuLabel,
  DropdownMenuSeparator,
}
