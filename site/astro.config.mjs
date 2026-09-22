// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import mermaid from "astro-mermaid";

// The docs under ../docs are the source of truth; hack/sync-docs.mjs copies them into
// src/content/docs before a build, so the site cannot drift from the repository.
export default defineConfig({
  // aramase.github.io carries a user-level custom domain, so project pages are served from
  // anishram.com and the github.io address only 301s there. Canonical links, og:url, and the
  // sitemap have to name the address people actually land on.
  site: "https://anishram.com",
  base: "/agentsessions",
  integrations: [
    // Must come before starlight so mermaid code fences are transformed before rendering.
    mermaid({
      theme: "neutral",
      autoTheme: true,
    }),
    starlight({
      title: "agentsessions",
      description:
        "A neutral contract for durable agent sessions: replay a run byte-for-byte without calling a model, fork it, and verify the record with a tool you wrote yourself.",
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/aramase/agentsessions" },
      ],
      editLink: {
        baseUrl: "https://github.com/aramase/agentsessions/edit/main/",
      },
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Concepts", slug: "concepts" },
            { label: "How you use it", slug: "interaction-model" },
            { label: "Quickstart", slug: "quickstart" },
          ],
        },
        {
          label: "Build on it",
          items: [
            { label: "Writing a harness", slug: "harness-authoring" },
            { label: "Architecture", slug: "architecture" },
            { label: "Running on agent-substrate", slug: "substrate-conformance" },
          ],
        },
        {
          label: "Operate it",
          items: [
            { label: "Security posture", slug: "security" },
            { label: "Observability", slug: "observability" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "API reference", slug: "api-reference" },
            { label: "FAQ", slug: "faq" },
          ],
        },
      ],
      components: {
        // Adds persistent nav links; splash pages have no sidebar, so without these the landing
        // page offers no route into the docs.
        SiteTitle: "./src/components/SiteTitle.astro",
      },
      customCss: ["./src/styles/custom.css"],
      lastUpdated: true,
    }),
  ],
});
