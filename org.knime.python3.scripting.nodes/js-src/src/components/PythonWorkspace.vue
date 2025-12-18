<script setup lang="ts">
import { type Ref, onMounted, onUnmounted, ref } from "vue";
import { useDebounceFn, useResizeObserver } from "@vueuse/core";

import { Button } from "@knime/components";

import {
  getPythonInitialData,
  pythonScriptingService,
} from "@/python-scripting-service";
import { usePythonPreviewStatusStore, useSessionStatusStore } from "@/store";

import PythonWorkspaceBody from "./PythonWorkspaceBody.vue";
import PythonWorkspaceHeader, {
  type ColumnSizes,
} from "./PythonWorkspaceHeader.vue";

const resizeContainer = ref<HTMLElement | null>(null);
const totalWidth: Ref<number> = ref(0);
const headerWidths = ref<ColumnSizes>([100, 100, 100]);

const useTotalWidth = () => {
  const rootResizeCallback = useDebounceFn((entries) => {
    const rect = entries[0].contentRect;
    totalWidth.value = rect.width;
  });
  const resizeObserver = useResizeObserver(resizeContainer, rootResizeCallback);

  onUnmounted(() => {
    resizeObserver.stop();
  });

  return { totalWidth, headerWidths };
};

const resetButtonEnabled: Ref<boolean> = ref(false);
const pythonPreviewStatus = usePythonPreviewStatusStore();
const resetWorkspace = async () => {
  const success = await pythonScriptingService.killInteractivePythonSession();

  // reset view
  pythonPreviewStatus.clearView();

  // set python execution status
  useSessionStatusStore().lastActionResult = success ? "RESET" : "RESET_FAILED";
};

onMounted(() => {
  useTotalWidth();

  const initialData = getPythonInitialData();
  if (
    initialData.inputConnectionInfo.every(
      (port) => port.isOptional || port.status === "OK",
    )
  ) {
    resetButtonEnabled.value = true;
  }
});
</script>

<template>
  <div ref="resizeContainer" class="container">
    <div ref="workspaceRef" class="workspace">
      <table>
        <PythonWorkspaceHeader
          :container-width="totalWidth"
          @update-header-widths="
            (e: ColumnSizes) => {
              headerWidths = e;
            }
          "
        />
        <PythonWorkspaceBody :column-widths="headerWidths" />
      </table>
    </div>
    <div class="controls">
      <Button
        class="reset-button"
        :with-border="false"
        compact
        :disabled="!resetButtonEnabled"
        @click="resetWorkspace"
      >
        Reset values
      </Button>
    </div>
  </div>
</template>

<style scoped lang="postcss">
.container {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  height: 100%;
  background-color: var(--knime-gray-ultra-light);
}

.workspace {
  --controls-height: 40px;

  position: relative;
  min-width: 120px;
  margin-top: 0;
  overflow: hidden auto;

  & table {
    flex: 1;
    height: calc(100% - var(--controls-height));
    font-family: "Roboto Mono", sans-serif;
    font-size: 13px;
    line-height: 24px;
    color: var(--knime-masala);
    text-align: left;
    border-collapse: collapse;
  }
}

.controls {
  display: flex;
  flex-direction: row-reverse;
  place-content: center space-between;
  min-height: var(--controls-height);
  max-height: var(--controls-height);
  overflow: hidden;
  border-top: 1px solid var(--knime-silver-sand);
}

.reset-button {
  height: 30px;
  margin-top: 5px;
  margin-right: 10px;
  margin-bottom: 5px;
}
</style>
@/python-initial-data-service
