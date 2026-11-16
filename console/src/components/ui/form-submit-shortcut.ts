import type { KeyboardEvent as ReactKeyboardEvent } from "react";
type FormKeyEvent = KeyboardEvent | ReactKeyboardEvent<HTMLElement>;

/** Submit the owning form or inline resource action bar. */
export function submitOwnedForm(
  target: HTMLElement,
  owner?: HTMLElement,
): boolean {
  let scope = owner ?? target.closest<HTMLElement>("form, [data-form-owner]");
  if (!scope) {
    let section = target.closest<HTMLElement>("section");
    while (section && !section.querySelector('[data-slot="dialog-footer"]'))
      section = section.parentElement?.closest<HTMLElement>("section") ?? null;
    scope = section;
  }
  if (!scope || !scope.contains(target)) return false;
  const form = target.closest("form");
  if (form && scope.contains(form)) {
    const submitter = form.querySelector<HTMLButtonElement | HTMLInputElement>(
      'button[type="submit"], input[type="submit"]',
    );
    if (!submitter || submitter.disabled) return true;
    form.requestSubmit(submitter);
    return true;
  }
  const footer = scope.querySelector<HTMLElement>(
    '[data-slot="dialog-footer"]',
  );
  const actions = footer?.querySelectorAll<HTMLButtonElement>("button");
  const submitter = actions?.item(actions.length - 1);
  if (!submitter) return false;
  if (!submitter.disabled && submitter.getAttribute("aria-disabled") !== "true")
    submitter.click();
  return true;
}

/** Nested controls retain their own keys; single-line inputs submit consistently. */
export function submitFormOnShortcut(
  event: FormKeyEvent,
  owner = event.currentTarget as HTMLElement,
  cancel?: () => void,
) {
  const native = "nativeEvent" in event ? event.nativeEvent : event;
  const target = event.target;
  if (
    event.defaultPrevented ||
    native.isComposing ||
    !(target instanceof HTMLElement) ||
    !owner.contains(target) ||
    target.closest(
      '.xterm, [data-form-shortcuts="ignore"], [role="listbox"], [role="menu"], .cm-search',
    )
  )
    return;
  if (target.closest('[role="combobox"][aria-expanded="true"]')) return;
  if (
    event.key === "Escape" &&
    cancel &&
    !event.altKey &&
    !event.ctrlKey &&
    !event.metaKey &&
    !event.shiftKey
  ) {
    if (
      target.closest(".workspace-fullscreen") ||
      target.closest(
        '[data-slot="dialog-content"], [data-slot="drawer-content"]',
      )
    )
      return;
    const footer = owner.querySelector<HTMLElement>(
      '[data-slot="dialog-footer"]',
    );
    if (footer?.querySelector<HTMLButtonElement>("button")?.disabled) return;
    event.preventDefault();
    cancel();
    return;
  }
  if (event.key !== "Enter" || event.altKey || event.shiftKey) return;
  const shortcut = event.ctrlKey || event.metaKey;
  if (
    !shortcut &&
    !(
      target instanceof HTMLInputElement &&
      ["email", "number", "password", "search", "tel", "text", "url"].includes(
        target.type,
      )
    )
  )
    return;
  if (!shortcut && target.closest("form")) return;
  if (submitOwnedForm(target, owner)) event.preventDefault();
}
