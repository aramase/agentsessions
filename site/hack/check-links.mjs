// Checks the built site for internal links that do not resolve.
//
// The docs are written for GitHub, where links are relative paths like (concepts.md#anchor).
// sync-docs.mjs rewrites those into site routes, and a regex rewrite is exactly the kind of thing
// that breaks quietly: the page still builds, the link just 404s. This runs over dist/ after a
// build and fails if any internal link has no target, or if a .md link survived the rewrite.

import { readdir, readFile } from "node:fs/promises";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";

const here = fileURLToPath(new URL(".", import.meta.url));
const dist = join(here, "..", "dist");
const base = "/agentsessions";

async function walk(dir) {
  const out = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...(await walk(full)));
    else if (entry.name.endsWith(".html")) out.push(full);
  }
  return out;
}

const htmlFiles = await walk(dist);

// Every directory holding an index.html is a routable page.
const routes = new Set([`${base}/`]);
for (const file of htmlFiles) {
  if (!file.endsWith(`${sep}index.html`)) continue;
  const dir = relative(dist, join(file, ".."));
  routes.add(dir === "" ? `${base}/` : `${base}/${dir.split(sep).join("/")}/`);
}

const failures = [];

for (const file of htmlFiles) {
  const html = await readFile(file, "utf8");
  const where = relative(dist, file);

  for (const [, href] of html.matchAll(/href="([^"]+)"/g)) {
    // A .md target means the rewrite missed it. GitHub links are intentional and fine.
    if (href.includes(".md") && !href.includes("github.com")) {
      failures.push({ where, href, why: "unrewritten .md link" });
      continue;
    }
    if (!href.startsWith(`${base}/`)) continue;

    const target = href.split("#")[0];
    if (/\.[a-z0-9]{2,5}$/i.test(target)) continue; // asset, emitted by the bundler
    const normalized = target.endsWith("/") ? target : `${target}/`;
    if (!routes.has(normalized)) {
      failures.push({ where, href: normalized, why: "no such route" });
    }
  }
}

if (failures.length > 0) {
  const seen = new Set();
  console.error("broken internal links:");
  for (const f of failures) {
    const key = `${f.href}|${f.why}`;
    if (seen.has(key)) continue;
    seen.add(key);
    console.error(`  [${f.why}] ${f.href}  (in ${f.where})`);
  }
  process.exit(1);
}

console.log(`checked ${htmlFiles.length} pages, ${routes.size} routes — all internal links resolve`);
