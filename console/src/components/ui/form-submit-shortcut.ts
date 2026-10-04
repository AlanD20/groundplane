import type { KeyboardEvent } from "react";

export function submitFormOnShortcut(event: KeyboardEvent<HTMLElement>) {
  if (
    event.defaultPrevented ||
    event.key !== "Enter" ||
    !(event.ctrlKey || event.metaKey) ||
    event.altKey ||
    event.shiftKey ||
    event.nativeEvent.isComposing ||
    !(event.target instanceof HTMLElement)
  )
    return;

  const form = event.target.closest("form");
  if (!form || !event.currentTarget.contains(form)) return;

  event.preventDefault();
  const submitter = form.querySelector<HTMLButtonElement | HTMLInputElement>(
    'button[type="submit"], input[type="submit"]',
  );
  if (!submitter || submitter.disabled) return;
  form.requestSubmit(submitter);
}
