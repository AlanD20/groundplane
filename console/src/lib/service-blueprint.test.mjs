import assert from "node:assert/strict";
import test from "node:test";
import { parse } from "yaml";
import {
  serviceBlueprintDocument,
  mergeServiceBlueprint,
} from "../features/blueprint/service-document.ts";

const source = `services:
  web: {image: 'example/web:1', command: ['sh', '-c', 'echo $VALUE'], environment: {FLAG: 'false', COUNT: '001'}, x-gp-release: {default_strategy: recreate}}
  worker: {image: 'example/worker:1', environment: {LARGE: 9007199254740993}}
networks: {app: {internal: true}}
volumes: {data: {}}
x-gp-scripts: {prepare: {script: "echo first\\necho second"}}
`;

// A Service-only edit must never omit shared resources or rewrite another Service.
test("Service edit preserves the full Blueprint and exact scalar values", () => {
  const snippet = serviceBlueprintDocument(source, "web");
  const draft = snippet.replace("example/web:1", "example/web:2");
  const actual = parse(mergeServiceBlueprint(source, draft, "web"), {
    intAsBigInt: true,
  });
  const expected = parse(source, { intAsBigInt: true });
  expected.services.web.image = "example/web:2";
  assert.deepEqual(actual, expected);
  assert.equal(actual.services.web.environment.FLAG, "false");
  assert.equal(actual.services.web.environment.COUNT, "001");
  assert.equal(actual.services.worker.environment.LARGE, 9007199254740993n);
  assert.ok(snippet.includes("  web:\n    image:"));
});

// Reject edits outside the visible Service before any validation/apply request.
test("Service editor rejects other resources, renamed Services and invalid YAML", () => {
  for (const draft of [
    "services: {web: {image: x}, worker: {image: y}}",
    "services: {other: {image: x}}",
    "services: {web: {image: x}}\nvolumes: {}",
    "services: {web: {image: x, image: y}}",
    "services: {web: *missing}",
    "services: {web: {image: x}}\n---\nservices: {}",
  ])
    assert.throws(() => mergeServiceBlueprint(source, draft, "web"));
});
