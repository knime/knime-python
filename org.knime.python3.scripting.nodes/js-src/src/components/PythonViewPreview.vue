<script setup lang="ts">
import { pythonScriptingService } from "@/python-scripting-service";
import { usePythonPreviewStatusStore } from "@/store";
import { onMounted, ref } from "vue";
import Button from "webapps-common/ui/components/Button.vue";

const iframe = ref<HTMLIFrameElement | null>(null);
const pythonPreviewStatus = usePythonPreviewStatusStore();

onMounted(() => {
  pythonPreviewStatus.updateViewCallback = () => {
    iframe.value?.contentWindow?.location.reload();
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
        src="./preview.html"
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
      <Button :with-border="true" @click="pythonScriptingService.runScript()"
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
