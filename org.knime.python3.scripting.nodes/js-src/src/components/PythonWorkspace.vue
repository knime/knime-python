<script setup lang="ts">
import { type Ref, computed, onMounted, onUnmounted, ref } from "vue";
import { useDebounceFn, useResizeObserver } from "@vueuse/core";

import { KdsButton, KdsEmptyState } from "@knime/kds-components";

import {
  getPythonInitialData,
  pythonScriptingService,
} from "@/python-scripting-service";
import {
  usePythonPreviewStatusStore,
  useSessionStatusStore,
  useWorkspaceStore,
} from "@/store";

import PythonWorkspaceBody from "./PythonWorkspaceBody.vue";
import PythonWorkspaceHeader, {
  type ColumnSizes,
} from "./PythonWorkspaceHeader.vue";

const resizeContainer = ref<HTMLElement | null>(null);
const totalWidth: Ref<number> = ref(0);
const headerWidths = ref<ColumnSizes>([100, 100, 100]);
// TODO KDS-690: Replace it once the JS tokens are available
const tableSidePadding = 8;

const useTotalWidth = () => {
  const rootResizeCallback = useDebounceFn((entries) => {
    const rect = entries[0].contentRect;
    totalWidth.value = rect.width - tableSidePadding * 2;
  });
  const resizeObserver = useResizeObserver(resizeContainer, rootResizeCallback);

  onUnmounted(() => {
    resizeObserver.stop();
  });

  return { totalWidth, headerWidths };
};

// check workspace store for emptiness to decide whether to show the workspace or the empty state
const workspaceStore = useWorkspaceStore();
const isEmpty = computed(() => (workspaceStore?.workspace?.length ?? 0) === 0);

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
  <div v-if="!isEmpty" ref="resizeContainer" class="container">
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
      <KdsButton
        class="reset-button"
        label="Reset Values"
        leading-icon="reset-all"
        variant="transparent"
        :disabled="!resetButtonEnabled"
        @click="resetWorkspace"
      />
    </div>
  </div>
  <div v-else class="container-empty-state">
    <KdsEmptyState
      headline="No temporary values yet"
      description="Run the Python script to display temporary values generated during execution."
    />
  </div>
</template>

<style scoped lang="postcss">
.container {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  height: 100%;
}

.container-empty-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
}

.workspace {
  --controls-height: 40px;

  position: relative;
  min-width: 120px;

  /* padding to match tableSidePadding in script */
  padding: var(--kds-spacing-container-0-5x) var(--kds-spacing-container-0-5x) 0
    var(--kds-spacing-container-0-5x);
  margin-top: 0;
  overflow: hidden auto;

  & table {
    flex: 1;
    max-width: 100%;
    height: calc(100% - var(--controls-height));
    border-collapse: collapse;
  }
}

.controls {
  display: flex;
  flex-direction: row-reverse;
  place-content: center space-between;
  align-items: center;
  height: var(--kds-dimension-component-height-2-25x);
  padding-right: var(--kds-spacing-container-0-25x);
  padding-left: var(--kds-spacing-container-0-25x);
  overflow: hidden;
  border-top: var(--kds-border-base-subtle);
}
</style>
