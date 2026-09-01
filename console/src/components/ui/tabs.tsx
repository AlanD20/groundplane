'use client'

import { Tabs as TabsPrimitive } from '@base-ui/react/tabs'
import { cn } from '@/lib/utils'

const Tabs = TabsPrimitive.Root

function TabsList({ className, ...props }: TabsPrimitive.List.Props) {
  return (
    <TabsPrimitive.List
      className={cn('relative flex items-center gap-0.5 overflow-x-auto border-b border-border', className)}
      {...props}
    />
  )
}

function TabsTab({ className, ...props }: TabsPrimitive.Tab.Props) {
  return (
    <TabsPrimitive.Tab
      className={cn(
        'relative inline-flex cursor-pointer select-none items-center gap-1.5 whitespace-nowrap rounded-t-lg border-b-2 border-transparent px-3 py-2 text-sm font-medium text-muted-foreground outline-none transition-all duration-150',
        'hover:bg-muted hover:text-foreground active:scale-[0.98]',
        'focus-visible:text-foreground focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring/50',
        'data-[selected]:border-primary data-[selected]:bg-surface data-[selected]:text-foreground data-[selected]:shadow-[inset_0_-1px_0_0_var(--color-primary)]',
        '[&_svg]:size-4',
        className,
      )}
      {...props}
    />
  )
}

function TabsPanel({ className, ...props }: TabsPrimitive.Panel.Props) {
  return <TabsPrimitive.Panel className={cn('outline-none', className)} {...props} />
}

const TabsTrigger = TabsTab
const TabsContent = TabsPanel

export { Tabs, TabsList, TabsTab, TabsPanel, TabsTrigger, TabsContent }
