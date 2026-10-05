"use client";

import { cn } from "@/lib/utils";
import { Tabs as TabsPrimitive } from "@base-ui/react/tabs";
import { createContext, useContext } from "react";

const Tabs = TabsPrimitive.Root;
const TabsStyle = createContext<"pills" | "underline">("pills");

function TabsList({
  className,
  variant = "pills",
  ...props
}: TabsPrimitive.List.Props & { variant?: "pills" | "underline" }) {
  return (
    <TabsStyle.Provider value={variant}>
      <TabsPrimitive.List
        className={cn(
          "relative flex items-center overflow-x-auto border-b border-border",
          variant === "underline" ? "gap-6 pb-0" : "gap-1.5 pb-3",
          className,
        )}
        {...props}
      />
    </TabsStyle.Provider>
  );
}

function TabsTab({ className, ...props }: TabsPrimitive.Tab.Props) {
  const variant = useContext(TabsStyle);
  return (
    <TabsPrimitive.Tab
      className={cn(
        "relative inline-flex cursor-pointer select-none items-center justify-center gap-1.5 whitespace-nowrap text-xs font-medium text-muted-foreground outline-none transition-colors duration-150 hover:text-foreground",
        "focus-visible:text-foreground focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring/70",
        variant === "underline"
          ? "rounded-none border-b-2 border-transparent px-0 pb-4 pt-0 font-normal data-[active]:border-primary data-[active]:text-primary"
          : "rounded-lg px-3.5 py-2.5 hover:bg-card/70 data-[active]:bg-accent data-[active]:text-primary",
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
