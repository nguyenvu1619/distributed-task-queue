<script setup lang="ts">
// Renders one ```mermaid block in the browser. mermaid is imported when the
// component mounts, so it is downloaded only on pages that have a diagram.
import { onMounted, ref, watch } from 'vue';
import { useData } from 'vitepress';

const props = defineProps<{ code: string }>();

const { isDark } = useData();
const source = decodeURIComponent(props.code);
const svg = ref('');
const failed = ref(false);

const uid = Math.random().toString(36).slice(2);
let renders = 0;

async function render(): Promise<void> {
  const n = ++renders;
  try {
    const { default: mermaid } = await import('mermaid');
    mermaid.initialize({ startOnLoad: false, theme: isDark.value ? 'dark' : 'default' });
    const result = await mermaid.render(`mermaid-${uid}-${n}`, source);
    // A theme switch can start a second render before the first finishes.
    if (n === renders) svg.value = result.svg;
  } catch (error) {
    console.error('Mermaid could not render a diagram:', error);
    failed.value = true;
  }
}

onMounted(render);
watch(isDark, render);
</script>

<template>
  <pre v-if="failed"><code>{{ source }}</code></pre>
  <div v-else class="mermaid-diagram" v-html="svg" />
</template>

<style scoped>
.mermaid-diagram {
  display: flex;
  justify-content: center;
  margin: 16px 0;
}
</style>
