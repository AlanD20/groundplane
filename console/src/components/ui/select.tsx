'use client'

import { Select as SelectPrimitive } from '@base-ui/react/select'
import { Check, ChevronsUpDown } from 'lucide-react'
import { cn } from '@/lib/utils'

export type SelectOption = { value: string; label: string }

function Select({
  value,
  onValueChange,
  options,
  placeholder = 'Select…',
  className,
  id,
}: {
  value?: string
  onValueChange?: (v: string) => void
  options: SelectOption[]
  placeholder?: string
  className?: string
  id?: string
}) {
  return (
    <SelectPrimitive.Root value={value} onValueChange={(v) => onValueChange?.(v as string)} items={options}>
      <SelectPrimitive.Trigger
        id={id}
        className={cn(
          'flex h-8 w-full items-center justify-between gap-2 rounded-lg border border-input bg-background px-2.5 text-sm outline-none transition-colors',
          'hover:bg-muted/50 focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/40',
          'data-[popup-open]:border-ring',
          className,
        )}
      >
        <SelectPrimitive.Value placeholder={placeholder} />
        <SelectPrimitive.Icon>
          <ChevronsUpDown className="size-3.5 text-muted-foreground" />
        </SelectPrimitive.Icon>
      </SelectPrimitive.Trigger>
      <SelectPrimitive.Portal>
        <SelectPrimitive.Positioner sideOffset={6} className="z-50 outline-none" alignItemWithTrigger={false}>
          <SelectPrimitive.Popup
            className={cn(
              'max-h-72 min-w-[var(--anchor-width)] overflow-y-auto rounded-lg border border-border bg-popover p-1 text-popover-foreground shadow-xl outline-none',
              'transition-all duration-100 data-[ending-style]:opacity-0 data-[starting-style]:opacity-0',
            )}
          >
            {options.map((opt) => (
              <SelectPrimitive.Item
                key={opt.value}
                value={opt.value}
                className="flex cursor-default items-center justify-between gap-2 rounded-md px-2 py-1.5 text-sm outline-none select-none data-[highlighted]:bg-muted data-[highlighted]:text-foreground"
              >
                <SelectPrimitive.ItemText>{opt.label}</SelectPrimitive.ItemText>
                <SelectPrimitive.ItemIndicator>
                  <Check className="size-3.5 text-primary" />
                </SelectPrimitive.ItemIndicator>
              </SelectPrimitive.Item>
            ))}
          </SelectPrimitive.Popup>
        </SelectPrimitive.Positioner>
      </SelectPrimitive.Portal>
    </SelectPrimitive.Root>
  )
}

export { Select }
