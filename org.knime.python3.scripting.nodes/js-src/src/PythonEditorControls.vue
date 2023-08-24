<script setup lang="ts">
import Button from "webapps-common/ui/components/Button.vue";
import { computed, ref } from "vue";

export type ExecutionStatus = {
  status: string;
  isRunning: boolean;
  isIdle: boolean;
  isError: boolean;
  selectedOnly: boolean;
};

const executionStatus = ref({
  status: "",
  isRunning: false,
  isIdle: true,
  isError: false,
  selectedOnly: false,
});

const mockTimeout = 2000;

const runSelected = async () => {
  if (executionStatus.value.isRunning) {
    // cancel mode
    executionStatus.value.status = "Execution cancelled.";
  } else {
    executionStatus.value.status = "";
  }

  executionStatus.value.isRunning = true;
  executionStatus.value.selectedOnly = true;
  executionStatus.value.isIdle = false;
  executionStatus.value.isError = false;

  await new Promise((r) => setTimeout(r, mockTimeout)).finally(() => {
    executionStatus.value.isRunning = false;
    executionStatus.value.isIdle = true;
    executionStatus.value.isError = false;
    executionStatus.value.status = "";
  });
};

const runAll = async () => {
  if (executionStatus.value.isRunning) {
    // cancel mode
    executionStatus.value.status = "Execution cancelled.";
  } else {
    executionStatus.value.status = "";
  }

  executionStatus.value.isRunning = true;
  executionStatus.value.selectedOnly = false;
  executionStatus.value.isIdle = false;
  executionStatus.value.isError = false;

  await new Promise((r) => setTimeout(r, mockTimeout)).finally(() => {
    executionStatus.value.isRunning = false;
    executionStatus.value.isIdle = true;
    executionStatus.value.isError = false;
    executionStatus.value.status = "";
  });
};

const runningAll = computed(
  () => executionStatus.value.isRunning && !executionStatus.value.selectedOnly,
);
const runningSelected = computed(
  () => executionStatus.value.isRunning && executionStatus.value.selectedOnly,
);
</script>

<template>
  <div class="status-text">
    <i>{{ executionStatus.status }}</i>
  </div>
  <div class="button-controls">
    <Button
      :disabled="runningAll"
      compact
      with-border
      class="left-button"
      @click="runSelected"
    >
      <div v-if="!runningSelected">Run Selected Lines</div>
      <div v-else><AbortIcon />Cancel</div>
    </Button>
    <Button
      :disabled="runningSelected"
      class="right-button"
      primary
      compact
      @click="runAll"
    >
      <div v-if="!runningAll"><PlayIcon />Run All</div>
      <div v-else><AbortIcon />Cancel</div>
    </Button>
  </div>
</template>

<style scoped lang="postcss">
.status-text {
  display: flex;
  justify-content: left;
  line-height: var(--controls-height);
  height: var(--controls-height);
  margin: -9px -20px;
}

.left-button {
  width: 150px;
}

.right-button {
  margin-left: 13px;
  width: 80px;
}
</style>
