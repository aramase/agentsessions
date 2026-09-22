// Copies the repository docs into the Starlight content directory.
//
// The files under ../../docs are the source of truth. Running this before a build means the site
// cannot drift from them, and it keeps the docs readable on GitHub, where most people will meet
// them first. Everything it writes is generated, so src/content/docs is gitignored.
//
// Two transformations are needed. Starlight requires frontmatter, which the plain markdown does not
// carry, and it serves pages at extensionless routes, so `concepts.md` has to become `/concepts/`.
import { copyFile, mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const docsDir = join(here, "..", "..", "docs");
const outDir = join(here, "..", "src", "content", "docs");
const repo = "https://github.com/aramase/agentsessions";

// README.md is the docs index; the Starlight sidebar replaces it, so it is not published.
const skip = new Set(["README.md"]);

// Descriptions are written here rather than derived from the first paragraph, which is usually a
// sentence of context rather than a summary and reads badly as a page description.
const meta = {
  "api-reference.md": {
    title: "API reference",
    description:
      "Every message, field, enum, and RPC in agentsessions.v1, generated from the proto comments.",
  },
  "architecture.md": {
    title: "Architecture",
    description:
      "How the neutral core is built: three contracts, a durable hash-chained log, and the determinism invariants the controller enforces.",
  },
  "concepts.md": {
    title: "Concepts",
    description:
      "The nouns: sessions, typed events, the log, incarnations, fences, resumability, and capabilities.",
  },
  "faq.md": {
    title: "FAQ",
    description: "What agentsessions is, what it is not, and how it behaves.",
  },
  "harness-authoring.md": {
    title: "Writing a harness",
    description:
      "Plug your agent into the api.Harness SPI: two methods, and the rules that keep replay exact.",
  },
  "interaction-model.md": {
    title: "How you use it",
    description:
      "What you implement, what you call, and what the project deliberately does not provide.",
  },
  "observability.md": {
    title: "Observability",
    description: "Structured operation logs, request correlation, and the data-safety contract.",
  },
  "quickstart.md": {
    title: "Quickstart",
    description:
      "Exec a turn, replay it with zero model calls, fork it, suspend and resume it, and verify the provenance chain.",
  },
  "security.md": {
    title: "Security posture",
    description:
      "What is protected, what is not, and how to deploy it accordingly. Read before exposing a host.",
  },
  "substrate-conformance.md": {
    title: "Running on agent-substrate",
    description:
      "Both capability tiers on a real cluster: stateless replay on gVisor and memory-snapshot continuity on a micro-VM.",
  },
};

/** Rewrite links so they resolve on the site instead of on GitHub. */
function rewriteLinks(markdown) {
  return (
    markdown
      // A sibling doc becomes a site route: (concepts.md#anchor) -> (/agentsessions/concepts/#anchor).
      // In the repository the link text is usually the bare filename, which reads as a broken
      // artifact on a rendered site, so swap it for the page title we already have.
      .replace(
        /\[([^\]]+)\]\((?!https?:|\/)([a-z0-9-]+)\.md(#[^)]*)?\)/g,
        (_m, text, name, anchor) => {
          const isFilename = text.replace(/`/g, "") === `${name}.md`;
          const label = isFilename ? (meta[`${name}.md`]?.title ?? text) : text;
          return `[${label}](/agentsessions/${name}/${anchor ?? ""})`;
        },
      )
      // Anything above docs/ only exists in the repository, so point at GitHub.
      .replace(/\]\(\.\.\/([^)]+)\)/g, (_m, path) => `](${repo}/blob/main/${path})`)
  );
}

/** Strip the leading H1; Starlight renders the title from frontmatter. */
function stripTitle(markdown) {
  return markdown.replace(/^#\s+.+\n+/, "");
}

/** Strip the generated-file banner, which is noise on a rendered page. */
function stripBanner(markdown) {
  return markdown.replace(/^<!--[\s\S]*?-->\n+/, "");
}

function yamlQuote(value) {
  return `"${value.replace(/"/g, '\\"')}"`;
}

// Markers that are fine in a working repository but must never reach a published page: they read
// as leaked engineering notes to anyone who did not write them. Failing the sync is deliberate, so
// the choice is made in the doc rather than silently shipped.
const internalMarkers = /^.*\b(TODO|FIXME|XXX|HACK)\b(\(|:).*$/gm;

function assertNoInternalMarkers(file, markdown) {
  const hits = markdown.match(internalMarkers);
  if (!hits) return;
  throw new Error(
    `docs/${file} contains internal markers that would be published:\n` +
      hits.map((h) => `    ${h.trim()}`).join("\n") +
      `\n  Resolve the note, move it to an issue, or rewrite it as prose that a reader can use.`,
  );
}

const files = (await readdir(docsDir)).filter((f) => f.endsWith(".md") && !skip.has(f));

await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });

for (const file of files) {
  const info = meta[file];
  if (!info) {
    throw new Error(
      `docs/${file} has no entry in hack/sync-docs.mjs. Add a title and description, ` +
        `or add it to the skip list, so a new doc cannot be published untitled.`,
    );
  }

  const source = await readFile(join(docsDir, file), "utf8");
  assertNoInternalMarkers(file, source);
  const body = rewriteLinks(stripTitle(stripBanner(source)));
  const frontmatter = [
    "---",
    `title: ${yamlQuote(info.title)}`,
    `description: ${yamlQuote(info.description)}`,
    "---",
    "",
  ].join("\n");

  await writeFile(join(outDir, file), frontmatter + body);
}

// The landing page is hand-written rather than synced from a doc, so it lives outside the generated
// directory and is copied in last. Everything under src/content/docs is generated, which is what
// makes wiping it safe.
await copyFile(join(here, "..", "src", "landing", "index.mdx"), join(outDir, "index.mdx"));

console.log(`synced ${files.length} docs + the landing page into src/content/docs`);
