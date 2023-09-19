<script setup lang="ts">
import { useWorkspaceStore } from "@/store";
import { ref, watch } from "vue";

const iframe = ref<HTMLIFrameElement | null>(null);

// NB: We watch on the workspace because it changes each time the user executes something
const workspaceStore = useWorkspaceStore();
watch(
  () => workspaceStore.workspace,
  () => {
    // Reload the iframe to get the new preview
    iframe.value?.contentWindow?.location.reload();
  },
);
</script>

<template>
  <iframe
    ref="iframe"
    title="Preview"
    sandbox="allow-scripts"
    src="./preview.html"
  />
</template>

<style scoped lang="postcss">
iframe {
  width: 100%;
  height: 100%;
  border: none;
  display: block;
}
</style>
