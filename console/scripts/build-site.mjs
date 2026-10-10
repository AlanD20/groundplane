import { build } from "vite";
import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const output = process.argv[2];
if (!output || !path.isAbsolute(output))
  throw new Error("An absolute staging directory is required");
const consoleRoot = path.resolve(import.meta.dirname, "..");
const configFile = path.join(consoleRoot, "vite.site.config.ts");
await build({ configFile, build: { outDir: output } });
const renderer = path.join(path.dirname(output), "renderer");
await build({
  configFile,
  build: {
    ssr: path.join(consoleRoot, "src/features/public-site/render.tsx"),
    outDir: renderer,
  },
});
const { render } = await import(
  pathToFileURL(path.join(renderer, "render.mjs")).href
);
const index = path.join(output, "index.html");
const html = await readFile(index, "utf8");
await writeFile(
  index,
  html.replace('<div id="root"></div>', `<div id="root">${render()}</div>`),
);
