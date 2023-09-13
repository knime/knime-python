<script setup lang="ts">
import Button from "webapps-common/ui/components/Button.vue";
import { computed, onMounted, onUnmounted, ref, type Ref } from "vue";
import { pythonScriptingService } from "../python-scripting-service";
import PlayIcon from "webapps-common/ui/assets/img/icons/play.svg";
import CancelIcon from "webapps-common/ui/assets/img/icons/circle-close.svg";
import LoadingIcon from "webapps-common/ui/components/LoadingIcon.vue";
import {
  handleExecutionInfo,
  handleSessionInfo,
} from "./utils/handleSessionInfo";
import { getScriptingService } from "@knime/scripting-editor";

const isRunningSupported = ref(false);

onMounted(async () => {
  if (await getScriptingService().inputsAvailable()) {
    isRunningSupported.value = true;

    handleSessionInfo(
      await pythonScriptingService.startInteractivePythonSession(),
    );
  } else {
    getScriptingService().sendToConsole({
      text: "Missing input data. Connect all input ports and execute preceeding nodes to enable script execution.",
    });
  }
});

onUnmounted(() => {
  if (isRunningSupported.value) {
    pythonScriptingService.killInteractivePythonSession();
  }
});

const runningSelected = ref(false);
const runningAll = ref(false);
const running = computed(() => {
  return runningSelected.value || runningAll.value;
});
const mouseOverRunAll = ref(false);
const mouseOverRunSelected = ref(false);

const buttonText = (
  initialText: string,
  buttonClickedAndRunning: Ref<boolean>,
  hovering: Ref<boolean>,
): string => {
  if (!buttonClickedAndRunning.value) {
    return initialText;
  }
  if (hovering.value) {
    return "Cancel";
  } else {
    return "Running ...";
  }
};

const runAllButtonText = computed((): string => {
  return buttonText("Run all", runningAll, mouseOverRunAll);
});

const runSelectedButtonText = computed((): string => {
  return buttonText(
    "Run selected lines",
    runningSelected,
    mouseOverRunSelected,
  );
});

const runButtonClicked = async (
  runningAllOrSelected: "runAll" | "runSelected",
  script: string | null,
  checkOutput: boolean = false,
) => {
  if (running.value) {
    handleSessionInfo(
      await pythonScriptingService.killInteractivePythonSession(),
    );
    handleSessionInfo(
      await pythonScriptingService.startInteractivePythonSession(),
    );
    runningAll.value = false;
    runningSelected.value = false;
  } else {
    if (script === null) {
      return;
    }
    const runButtonRef =
      runningAllOrSelected === "runAll" ? runningAll : runningSelected;
    runButtonRef.value = true;
    if (runningAllOrSelected === "runAll") {
      handleSessionInfo(
        await pythonScriptingService.startInteractivePythonSession(),
      );
    }
    handleSessionInfo({
      status: "RUNNING",
      description: "Running script in active session",
    });
    const executionInfo = await pythonScriptingService.runScript(
      script,
      checkOutput,
    );
    handleSessionInfo(executionInfo);
    handleExecutionInfo(executionInfo);
    runButtonRef.value = false;
  }
};

const onHoverRunButton = (
  button: "runAll" | "runSelected",
  type: "mouseover" | "mouseleave",
) => {
  const buttonRef =
    button === "runAll" ? mouseOverRunAll : mouseOverRunSelected;
  buttonRef.value = type === "mouseover";
};
</script>

<template>
  <div>
    <Button
      compact
      with-border
      :disabled="!isRunningSupported || (running && !runningSelected)"
      class="run-selected-button"
      @click="
        runButtonClicked(
          'runSelected',
          pythonScriptingService.getSelectedLines(),
        )
      "
      @mouseover="onHoverRunButton('runSelected', 'mouseover')"
      @mouseleave="onHoverRunButton('runSelected', 'mouseleave')"
    >
      <div>
        <PlayIcon v-if="!runningSelected" />
        <LoadingIcon v-else-if="!mouseOverRunSelected" class="spinning" />
        <CancelIcon v-else />
        {{ runSelectedButtonText }}
      </div>
    </Button>
    <Button
      :disabled="!isRunningSupported || (running && !runningAll)"
      class="run-all-button"
      primary
      compact
      @click="runButtonClicked('runAll', pythonScriptingService.getAllLines())"
      @mouseover="onHoverRunButton('runAll', 'mouseover')"
      @mouseleave="onHoverRunButton('runAll', 'mouseleave')"
    >
      <div>
        <PlayIcon v-if="!runningAll" />
        <LoadingIcon v-else-if="!mouseOverRunAll" class="spinning" />
        <CancelIcon v-else />
        {{ runAllButtonText }}
      </div>
    </Button>
  </div>
</template>

<style scoped lang="postcss">
.run-selected-button {
  width: 180px;
}

.run-all-button {
  margin-left: 13px;
  width: 120px;
}
</style>
