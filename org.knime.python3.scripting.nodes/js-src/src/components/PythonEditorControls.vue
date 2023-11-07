<script setup lang="ts">
import { getScriptingService, useEditorStore } from "@knime/scripting-editor";
import { computed, onMounted, onUnmounted, ref, type Ref } from "vue";
import CancelIcon from "webapps-common/ui/assets/img/icons/circle-close.svg";
import PlayIcon from "webapps-common/ui/assets/img/icons/play.svg";
import Button from "webapps-common/ui/components/Button.vue";
import LoadingIcon from "webapps-common/ui/components/LoadingIcon.vue";
import { pythonScriptingService } from "../python-scripting-service";
import { useExecutableSelectionStore, useSessionStatusStore } from "@/store";

const editorStore = useEditorStore();
const hasSelection = computed(() => editorStore.selection !== "");

const isRunningSupported = ref(false);

onMounted(async () => {
  if (await getScriptingService().inputsAvailable()) {
    isRunningSupported.value = true;
  } else {
    getScriptingService().sendToConsole({
      warning:
        "Missing input data. Connect all input ports and execute preceding nodes to enable script execution.",
    });
  }
});

onUnmounted(() => {
  if (isRunningSupported.value) {
    pythonScriptingService.killInteractivePythonSession();
  }
});

const sessionStatus = useSessionStatusStore();

// TODO: show button text on correct button when running from view placeholder AP-21485
const cancelOnButton = ref<"runAll" | "runSelected">("runAll");
const running = computed(() => sessionStatus.status === "RUNNING");
const runningSelected = computed(
  () => running.value && cancelOnButton.value === "runSelected",
);
const runningAll = computed(
  () => running.value && cancelOnButton.value === "runAll",
);
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
  } else {
    // Remember which button was clicked - to show the Cancel text there
    cancelOnButton.value = runningAllOrSelected;
    if (runningAllOrSelected === "runAll") {
      pythonScriptingService.runScript();
    } else {
      pythonScriptingService.runSelectedLines();
    }
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
        !isRunningSupported ||
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
        !isRunningSupported ||
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
