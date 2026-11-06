import { defineAsyncComponent } from 'vue';
import DefaultTheme from 'vitepress/theme';
import type { Theme } from 'vitepress';

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    // Registered as an async component on purpose. VitePress preloads every
    // dynamic import of the app bundle on every page, so a component imported
    // here directly would drag mermaid into all of them.
    app.component(
      'MermaidDiagram',
      defineAsyncComponent(() => import('./MermaidDiagram.vue')),
    );
  },
} satisfies Theme;
