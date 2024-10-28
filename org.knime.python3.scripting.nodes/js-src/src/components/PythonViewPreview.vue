<script setup lang="ts">
import { onMounted, ref } from "vue";

import { Button } from "@knime/components";

import { pythonScriptingService } from "@/python-scripting-service";
import { usePythonPreviewStatusStore, useSessionStatusStore } from "@/store";

const IFRAME_SOURCE = "./preview.html";

const iframe = ref<HTMLIFrameElement | null>(null);
const pythonPreviewStatus = usePythonPreviewStatusStore();
const sessionStatus = useSessionStatusStore();

onMounted(() => {
  pythonPreviewStatus.updateViewCallback = () => {
    // NB: This is a workaround to force the iframe to reload the content
    // "replace" is allowed by the same-origin policy on the sandboxed iframe
    // see https://developer.mozilla.org/en-US/docs/Web/Security/Same-origin_policy#location
    iframe.value?.contentWindow?.location.replace(IFRAME_SOURCE);
  };
});
</script>

<template>
  <div class="container">
    <div v-show="pythonPreviewStatus.hasValidView" class="iframe-container">
      <iframe
        ref="iframe"
        title="Preview"
        sandbox="allow-scripts"
        :src="IFRAME_SOURCE"
      />
    </div>
    <div
      v-show="!pythonPreviewStatus.hasValidView"
      class="placeholder-container"
    >
      <div v-if="!pythonPreviewStatus.isExecutedOnce" class="placeholder-text">
        Please run the code to see the preview.
      </div>
      <div v-else class="placeholder-text">
        The view cannot be displayed. Please check the error message and
        re-execute the script.
      </div>
      <img id="preview-img" src="/assets/plot-placeholder.svg" />
      <Button
        :with-border="true"
        :disabled="
          !sessionStatus.isRunningSupported ||
          sessionStatus.status === 'RUNNING_ALL' ||
          sessionStatus.status === 'RUNNING_SELECTED'
        "
        @click="pythonScriptingService.runScript()"
        >Run code</Button
      >
    </div>
  </div>
</template>

<style scoped lang="postcss">
iframe,
.iframe-container,
.container {
  width: 100%;
  height: 100%;
  border: none;
  display: block;
}

.placeholder-container {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  gap: 20px;
  min-width: 15vw;
}

#preview-img {
  max-width: 200px;
  height: auto;
  opacity: 0.3;
}

.placeholder-text {
  margin-left: 20px;
  margin-right: 20px;
}
</style>
