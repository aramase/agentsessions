# site

The [agentsessions.dev site](https://aramase.github.io/agentsessions), built with Astro and
Starlight and deployed to GitHub Pages by `.github/workflows/site.yml`.

`../docs` is the source of truth. `hack/sync-docs.mjs` copies those files into
`src/content/docs/` before every build, adding frontmatter and rewriting GitHub-relative links into
site routes, so the published site cannot drift from the repository. That directory is generated and
gitignored; do not edit it.

Edit `../docs/*.md` for documentation. Edit `src/landing/index.mdx` for the landing page. A new doc
needs an entry in the `meta` map in `hack/sync-docs.mjs` and a sidebar entry in `astro.config.mjs`;
the sync fails loudly if the first is missing.

```bash
npm ci
npm run dev     # syncs docs, then serves with live reload
npm run build   # syncs docs, then builds into dist/
node hack/check-links.mjs   # after a build: fails on internal links that do not resolve
```
