"use client";

import { cn } from "@/lib/utils";
import { Tabs as TabsPrimitive } from "@base-ui/react/tabs";

const Tabs = TabsPrimitive.Root;

function TabsList({ className, ...props }: TabsPrimitive.List.Props) {
  return (
    <TabsPrimitive.List
      className={cn(
        "relative flex items-center gap-1 overflow-x-auto rounded-lg border border-border bg-muted/50 p-1",
        className,
      )}
      {...props}
    />
  );
}

function TabsTab({ className, ...props }: TabsPrimitive.Tab.Props) {
  return (
    <TabsPrimitive.Tab
      className={cn(
        "relative inline-flex cursor-pointer select-none items-center justify-center gap-1.5 whitespace-nowrap rounded-md border border-transparent px-3 py-2 text-xs font-medium text-muted-foreground outline-none transition-colors duration-150",
        "hover:bg-card/70 hover:text-foreground",
        "focus-visible:text-foreground focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring/70",
        "data-[active]:border-primary/25 data-[active]:bg-card data-[active]:text-primary data-[active]:shadow-sm",
        "[&_svg]:size-4",
        className,
      )}
      {...props}
    />
  );
}

function TabsPanel({ className, ...props }: TabsPrimitive.Panel.Props) {
  return (
    <TabsPrimitive.Panel className={cn("outline-none", className)} {...props} />
  );
}

export { Tabs, TabsList, TabsPanel, TabsTab };
