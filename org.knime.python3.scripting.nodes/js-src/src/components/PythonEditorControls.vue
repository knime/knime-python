<script setup lang="ts">
import { computed, ref, type Ref } from "vue";
import CancelIcon from "@knime/styles/img/icons/circle-close.svg";
import PlayIcon from "@knime/styles/img/icons/play.svg";
import { Button, LoadingIcon } from "@knime/components";
import { pythonScriptingService } from "../python-scripting-service";
import { useExecutableSelectionStore, useSessionStatusStore } from "@/store";
import { editor } from "@knime/scripting-editor";

const mainEditorState = editor.useMainCodeEditorStore();
const hasSelection = computed(
  () => mainEditorState.value?.selection.value !== "",
);

const sessionStatus = useSessionStatusStore();

const runningSelected = computed(
  () => sessionStatus.status === "RUNNING_SELECTED",
);
const runningAll = computed(() => sessionStatus.status === "RUNNING_ALL");
const running = computed(() => runningAll.value || runningSelected.value);
const mouseOverRunAll = ref(false);
const mouseOverRunSelected = ref(false);
const executableSelection = useExecutableSelectionStore();

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

const runButtonClicked = (runningAllOrSelected: "runAll" | "runSelected") => {
  if (running.value) {
    // NB: The service will change the session status and handle errors
    pythonScriptingService.killInteractivePythonSession();
  } else if (runningAllOrSelected === "runAll") {
    pythonScriptingService.runScript();
  } else {
    pythonScriptingService.runSelectedLines();
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
      :disabled="
        !sessionStatus.isRunningSupported ||
        (running && !runningSelected) ||
        executableSelection.isMissing ||
        !hasSelection
      "
      class="run-selected-button"
      @click="runButtonClicked('runSelected')"
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
      :disabled="
        !sessionStatus.isRunningSupported ||
        (running && !runningAll) ||
        executableSelection.isMissing
      "
      class="run-all-button"
      primary
      compact
      @click="runButtonClicked('runAll')"
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
