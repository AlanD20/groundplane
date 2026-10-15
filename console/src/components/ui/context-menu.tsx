import { ContextMenu as ContextMenuPrimitive } from "@base-ui/react/context-menu";
import {
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
} from "./dropdown-menu";

// Base UI context menus share the Menu popup primitives and Console styling.
export const ContextMenu = ContextMenuPrimitive.Root;
export const ContextMenuTrigger = ContextMenuPrimitive.Trigger;
export const ContextMenuContent = DropdownMenuContent;
export const ContextMenuItem = DropdownMenuItem;
export const ContextMenuSeparator = DropdownMenuSeparator;
