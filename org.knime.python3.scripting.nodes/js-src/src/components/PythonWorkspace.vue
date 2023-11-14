<script setup lang="ts">
import { useDebounceFn, useResizeObserver } from "@vueuse/core";
import { onMounted, onUnmounted, ref, type Ref } from "vue";
import Button from "webapps-common/ui/components/Button.vue";

import { pythonScriptingService } from "@/python-scripting-service";
import { usePythonPreviewStatusStore } from "@/store";
import { getScriptingService } from "@knime/scripting-editor";
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
const resetWorkspace = () => {
  pythonScriptingService.killInteractivePythonSession();

  // reset view
  pythonPreviewStatus.clearView();
};

onMounted(async () => {
  useTotalWidth();
  if (await getScriptingService().inputsAvailable()) {
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
  background-color: var(--knime-gray-ultra-light);
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.workspace {
  --controls-height: 40px;

  position: relative;
  overflow-y: auto;
  overflow-x: hidden;
  min-width: 120px;
  margin-top: 0;

  & table {
    border-collapse: collapse;
    font-family: "Roboto Mono", sans-serif;
    font-size: 13px;
    text-align: left;
    line-height: 24px;
    flex: 1;
    color: var(--knime-masala);
    height: calc(100% - var(--controls-height));
  }
}

.controls {
  display: flex;
  overflow: hidden;
  flex-direction: row-reverse;
  justify-content: space-between;
  align-content: center;
  min-height: var(--controls-height);
  max-height: var(--controls-height);
  height: var(--controls-height);
  background-color: var(--knime-gray-light-semi);
  border-top: 1px solid var(--knime-silver-sand);
}

.reset-button {
  height: 30px;
  margin-top: 5px;
  margin-bottom: 5px;
  margin-right: 10px;
}
</style>
