import { defineConfig } from 'vitepress';

const repo = 'https://github.com/nguyenvu1619/distributed-task-queue';

export default defineConfig({
  lang: 'en-US',
  title: 'distributed-task-queue',
  description: 'A job queue for Node.js and TypeScript that runs entirely on PostgreSQL.',

  // Served from https://nguyenvu1619.github.io/distributed-task-queue/, so
  // every asset URL needs the repository name in front of it.
  base: '/distributed-task-queue/',

  markdown: {
    // ```mermaid blocks become <MermaidDiagram>, which draws them in the
    // browser (theme/MermaidDiagram.vue). This runs after VitePress's own
    // fence plugins, so those never see a mermaid block.
    config(md) {
      const fence = md.renderer.rules.fence!;
      md.renderer.rules.fence = (tokens, idx, options, env, self) => {
        const { info, content } = tokens[idx];
        if (info.trim() === 'mermaid') {
          return `<MermaidDiagram code="${encodeURIComponent(content)}" />`;
        }
        return fence(tokens, idx, options, env, self);
      };
    },
  },

  themeConfig: {
    nav: [
      { text: 'Quick start', link: '/getting-started/quick-start' },
      { text: 'API', link: '/reference/api' },
    ],

    // Same order as the contents list in index.md.
    sidebar: [
      {
        text: 'Getting started',
        items: [
          { text: 'Introduction', link: '/' },
          { text: 'Installation', link: '/getting-started/installation' },
          { text: 'Quick start', link: '/getting-started/quick-start' },
        ],
      },
      {
        text: 'Concepts',
        items: [
          { text: 'How it works', link: '/concepts/how-it-works' },
          { text: 'Job lifecycle', link: '/concepts/job-lifecycle' },
          { text: 'Concurrency control', link: '/concepts/concurrency-control' },
        ],
      },
      {
        text: 'Guides',
        items: [
          { text: 'Queues', link: '/guides/queues' },
          { text: 'Publishing jobs', link: '/guides/publishing-jobs' },
          { text: 'Workers', link: '/guides/workers' },
          { text: 'Crash recovery', link: '/guides/crash-recovery' },
          { text: 'Running in production', link: '/guides/production' },
        ],
      },
      {
        text: 'Reference',
        items: [
          { text: 'API', link: '/reference/api' },
          { text: 'Errors', link: '/reference/errors' },
          { text: 'Database schema', link: '/reference/database-schema' },
        ],
      },
      { text: 'Known limitations', link: '/known-limitations' },
    ],

    outline: { level: [2, 3] },
    search: { provider: 'local' },
    socialLinks: [{ icon: 'github', link: repo }],
    editLink: {
      pattern: `${repo}/edit/main/docs/:path`,
      text: 'Edit this page on GitHub',
    },
  },
});
