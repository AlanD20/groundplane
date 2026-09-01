'use client'

import { Switch as SwitchPrimitive } from '@base-ui/react/switch'
import { cn } from '@/lib/utils'

function Switch({ className, ...props }: SwitchPrimitive.Root.Props) {
  return (
    <SwitchPrimitive.Root
      className={cn(
        'relative inline-flex h-5 w-9 shrink-0 cursor-pointer items-center rounded-full border border-transparent bg-input outline-none transition-colors',
        'data-[checked]:bg-primary focus-visible:ring-3 focus-visible:ring-ring/40',
        className,
      )}
      {...props}
    >
      <SwitchPrimitive.Thumb className="pointer-events-none block size-4 translate-x-0.5 rounded-full bg-background shadow transition-transform data-[checked]:translate-x-[18px]" />
    </SwitchPrimitive.Root>
  )
}

export { Switch }
