import { Document, isMap, parseDocument, visit } from "yaml";

function readDocument(source: string) {
  const document = parseDocument(source, { intAsBigInt: true });
  if (document.errors.length) throw new Error(document.errors[0].message);
  if (document.warnings.length) throw new Error(document.warnings[0].message);
  if (!isMap(document.contents))
    throw new Error("Blueprint must be a YAML mapping.");
  return document;
}

function serviceNode(document: Document, name: string) {
  const node = document.getIn(["services", name], true);
  if (!isMap(node))
    throw new Error(`Blueprint must contain the Service ${name}.`);
  return node;
}

/** The saved Blueprint, including GP extensions, owns the editable Service. */
export function serviceBlueprintDocument(source: string, name: string): string {
  const document = readDocument(source);
  const service = serviceNode(document, name).clone();
  const result = new Document({ services: {} });
  result.setIn(["services", name], service);
  visit(result, {
    Collection(_key, node) {
      node.flow = node.items.length === 0;
    },
  });
  return result.toString({ indent: 2, lineWidth: 100 });
}

/** Replace only the selected Service; never submit a fragment as the Environment. */
export function mergeServiceBlueprint(
  source: string,
  draft: string,
  name: string,
): string {
  const document = readDocument(source);
  serviceNode(document, name);
  const edited = readDocument(draft);
  const services = edited.get("services", true);
  if (
    !isMap(edited.contents) ||
    edited.contents.items.length !== 1 ||
    !isMap(services) ||
    services.items.length !== 1
  ) {
    throw new Error(
      "Edit only this Service here. Use the Environment Blueprint for shared resources or other Services.",
    );
  }
  const replacement = serviceNode(edited, name);
  // Resolve aliases against the draft itself before inserting it into another document.
  // This rejects references to anchors that are absent from the visible editor.
  edited.toJS({ maxAliasCount: 1000 });
  document.setIn(["services", name], replacement.clone());
  return document.toString({ indent: 2, lineWidth: 100 });
}
