const documents = new WeakMap<Element, () => string>();

export function registerEditorDraft(element: Element, read: () => string) {
  documents.set(element, read);
  return () => {
    documents.delete(element);
  };
}

export function editorDraftValue(element: Element): string {
  return documents.get(element)?.() ?? "";
}
