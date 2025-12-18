<script setup lang="ts">
import { computed } from "vue";

import { useSessionStatusStore } from "@/store";

const sessionStatus = useSessionStatusStore();

const errorState = { text: "Execution failed", class: "error" };
const lastActionStatus = {
  RUNNING: { text: "Running...", class: "default" },
  SUCCESS: { text: "Execution successful", class: "success" },
  CANCELLED: { text: "Execution cancelled", class: "cancel" },
  KNIME_ERROR: errorState,
  FATAL_ERROR: errorState,
  EXECUTION_ERROR: errorState,
  RESET_FAILED: errorState,
  RESET: { text: "Values reset successfully", class: "success" },
  IDLE: { text: "", class: "hidden" },
};

const executionStatusText = computed(() => {
  if (sessionStatus.status.includes("RUNNING")) {
    return lastActionStatus.RUNNING;
  }
  const lastExecutionResult = sessionStatus.lastActionResult;
  if (lastExecutionResult) {
    return lastActionStatus[lastExecutionResult];
  }
  return lastActionStatus.IDLE;
});
</script>

<template>
  <div class="status-wrapper" :class="{ [executionStatusText.class]: true }">
    <div class="dot" />
    <div>{{ executionStatusText.text }}</div>
  </div>
</template>

<style scoped lang="postcss">
.default {
  --status-color: var(--knime-dove-gray);
}

.success {
  --status-color: var(--knime-meadow);
}

.error,
.cancel {
  --status-color: var(--knime-coral-dark);
}

.hidden {
  display: none;
}

.status-wrapper {
  display: flex;
  flex-direction: row;
  gap: 10px;
  align-items: center;
  justify-content: left;
  width: 180px;
  color: var(--status-color);
}

.dot {
  width: 5px;
  height: 5px;
  background-color: var(--status-color);
  border-radius: 50%;
}
</style>
