"use client";

import { Button } from "@/components/ui/button";

import { useEffect, useRef, useState, type ReactNode } from "react";
import { Check, Copy, AlertCircle } from "lucide-react";
import { cn } from "@/lib/utils";

async function copyText(value: string) {
  if (navigator.clipboard) {
    await navigator.clipboard.writeText(value);
    return;
  }
  // The LAN Console supports HTTP, where the Clipboard API is unavailable.
  const active =
    document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
  const selection = window.getSelection();
  const ranges = selection
    ? Array.from({ length: selection.rangeCount }, (_, i) =>
        selection.getRangeAt(i).cloneRange(),
      )
    : [];
  const input = document.createElement("textarea");
  input.value = value;
  input.readOnly = true;
  input.style.position = "fixed";
  input.style.opacity = "0";
  input.style.pointerEvents = "none";
  // Keep the temporary selection inside a modal's focus boundary.
  const container =
    active?.closest('[role="dialog"], [role="alertdialog"]') ?? document.body;
  container.append(input);
  try {
    input.focus({ preventScroll: true });
    input.select();
    if (!document.execCommand("copy"))
      throw new Error("Clipboard copy was refused");
  } finally {
    input.remove();
    active?.focus({ preventScroll: true });
    selection?.removeAllRanges();
    ranges.forEach((range) => selection?.addRange(range));
  }
}

export function CopyButton({
  value,
  className,
  label,
  children,
  iconOnly = false,
}: {
  value: string;
  className?: string;
  label?: string;
  children?: ReactNode;
  iconOnly?: boolean;
}) {
  const [status, setStatus] = useState<
    "idle" | "copying" | "copied" | "failed"
  >("idle");
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  useEffect(() => () => clearTimeout(timer.current), []);
  const copied = status === "copied";
  const failed = status === "failed";
  const actionLabel = failed
    ? "Copy failed. Select the text and copy manually."
    : copied
      ? "Copied"
      : (label ?? "Copy");
  return (
    <Button
      variant="ghost"
      size="content"
      type="button"
      disabled={status === "copying"}
      onClick={async (event) => {
        event.stopPropagation();
        clearTimeout(timer.current);
        setStatus("copying");
        try {
          await copyText(value);
          setStatus("copied");
          timer.current = setTimeout(() => setStatus("idle"), 1400);
        } catch {
          setStatus("failed");
        }
      }}
      className={cn(
        "inline-flex items-center gap-1.5 rounded-md p-1.5 text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-1 focus-visible:ring-ring",
        className,
      )}
      aria-label={actionLabel}
      title={actionLabel}
    >
      {children && (
        <span className="min-w-0 break-all text-left">{children}</span>
      )}
      {failed ? (
        <AlertCircle className="size-3.5 text-destructive" />
      ) : copied ? (
        <Check className="size-3.5 text-success" />
      ) : (
        <Copy className="size-3.5" />
      )}
      {label && !children && !iconOnly && (
        <span className="text-xs">
          {copied ? "Copied" : failed ? "Copy failed" : label}
        </span>
      )}
      <span className="sr-only" role="status">
        {copied
          ? "Copied to clipboard"
          : failed
            ? "Copy failed. Select the text and copy manually."
            : ""}
      </span>
    </Button>
  );
}
