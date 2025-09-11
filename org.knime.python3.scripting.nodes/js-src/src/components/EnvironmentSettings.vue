<script setup lang="ts">
import { type Ref, computed, onMounted, onUnmounted, ref, watch } from "vue";

import { Dropdown, Label, RadioButtons } from "@knime/components";
import { getSettingsService } from "@knime/scripting-editor";

import { getPythonInitialDataService } from "@/python-initial-data-service";
import { pythonScriptingService } from "@/python-scripting-service";
import {
  setSelectedExecutable,
  useExecutableSelectionStore,
  useExecutableSettingStore,
} from "@/store";
import type { ExecutableOption } from "@/types/common";

const executableSelection = useExecutableSelectionStore();
const selectedEnv: Ref<"default" | "conda"> = ref(
  executableSelection.id === "" ? "default" : "conda",
);
const selectedExecutableOption = ref("");
const dropDownOptions: Ref<
  { id: string; text: string; type: string }[] | null
> = ref(null);

const hasDropDownOptions = computed(() => {
  return dropDownOptions.value !== null && dropDownOptions.value.length > 0;
});

const isMissingVarSelected = computed(() => {
  return (
    dropDownOptions.value?.find(
      ({ id }) => id === selectedExecutableOption.value,
    )?.type === "MISSING_VAR"
  );
});

let executableOptions: ExecutableOption[];

const getExecutableById = (executableId: string): ExecutableOption | null => {
  return executableOptions?.find(({ id }) => id === executableId) ?? null;
};

onMounted(async () => {
  executableOptions =
    getPythonInitialDataService().getInitialData().executableOptionsList;

  dropDownOptions.value = executableOptions
    .map((executableOption: ExecutableOption) => ({
      id: executableOption.id,
      text: executableOption.id,
      type: executableOption.type as any,
    }))
    .filter(({ id }) => id !== "");

  if (executableSelection.id !== "") {
    selectedExecutableOption.value = executableSelection.id;
  } else if (hasDropDownOptions.value) {
    selectedExecutableOption.value = dropDownOptions.value[0].id;
  }

  // Set the settings store to the initially selected flow variable
  const executableSettingState = useExecutableSettingStore();
  if (executableSettingState.value === null) {
    const register = await getSettingsService().registerSettings("model");
    const environmentSettingState = register("");
    if (executableSelection.livePrefValue === "") {
      executableSettingState.value =
        environmentSettingState.addControllingFlowVariable(null);
    } else {
      executableSettingState.value =
        environmentSettingState.addControllingFlowVariable(
          executableSelection.livePrefValue,
        );
    }
  }
});

// Note that the Ok button will not trigger this function
onUnmounted(() => {
  if (
    executableSelection.livePrefValue !== null && // if something is set
    executableSelection.id !== executableSelection.livePrefValue // and it differs from the old setting
  ) {
    // Update the selected executable in the store
    setSelectedExecutable({
      id: executableSelection.livePrefValue,
      isMissing: false,
      livePrefValue: null,
    });

    // Notify the backend that the executable selection has changed
    pythonScriptingService.updateExecutableSelection(executableSelection.id);

    // Print a message about the changed executable to the console
    const executable = getExecutableById(executableSelection.id);
    if (executable) {
      pythonScriptingService.sendToConsole({
        text: `Changed python executable to ${executable.pythonExecutable}`,
      });
    }
  }
});

const updateLiveExecutableSelection = () => {
  const newSelection =
    selectedEnv.value === "default"
      ? ""
      : (selectedExecutableOption.value ?? "");

  if (isMissingVarSelected.value && selectedEnv.value === "conda") {
    return;
  }
  executableSelection.livePrefValue = newSelection;

  // Update the settings store to let the parent know about the dirty state
  if (
    executableSelection.livePrefValue === "" ||
    executableSelection.livePrefValue === null
  ) {
    useExecutableSettingStore().value?.unset();
  } else {
    useExecutableSettingStore().value?.set(executableSelection.livePrefValue);
  }
};

watch(selectedEnv, updateLiveExecutableSelection);
watch(selectedExecutableOption, updateLiveExecutableSelection);
</script>

<template>
  <div class="container">
    <RadioButtons
      v-model="selectedEnv"
      class="radio-buttons"
      alignment="vertical"
      :possible-values="[
        {
          id: 'default',
          text: 'Python environment selected in KNIME preferences',
          subtext:
            'Choose this option for projects that do not require specific package versions or isolated environments, ' +
            'or if you want to use the same Python environment in all your workflows.',
        },
        {
          id: 'conda',
          text: 'Conda environment with Conda Environment Propagation Node',
          subtext:
            'Choose this option to use a dedicated and shareable Python environment for this node.',
        },
      ]"
    />
    <div
      v-if="selectedEnv === 'conda' && dropDownOptions !== null"
      class="flow-variable-selection"
    >
      <Label #default="{ flowVarDropdown }" text="Conda flow variable">
        <!-- eslint-disable vue/attribute-hyphenation typescript complains with ':aria-label' instead of ':ariaLabel'-->
        <Dropdown
          :id="flowVarDropdown"
          v-model="selectedExecutableOption"
          class="flow-var-dropdown"
          :ariaLabel="'Executable Selection Dropdown'"
          :is-valid="hasDropDownOptions && !isMissingVarSelected"
          :disabled="!hasDropDownOptions"
          placeholder="Select flow variable"
          :possible-values="dropDownOptions"
        />
      </Label>
      <div v-if="!hasDropDownOptions" class="error-text">
        Connect Conda Environment Propagation Node to Python Script Node.
      </div>
      <div v-if="isMissingVarSelected" class="error-text">
        The currently selected flow variable is not available.
      </div>
    </div>
  </div>
</template>

<style scoped lang="postcss">
.container {
  margin-left: 80px;
  margin-top: 80px;

  & .flow-variable-selection {
    width: 400px;
    margin-left: 25px;

    & .error-text {
      color: var(--knime-coral-dark);
      font-size: 12px;
      line-height: 14px;
      margin-top: 8px;
    }
  }
}

.radio-buttons {
  --radio-button-margin: 0 0 30px 0;
}
</style>
@/python-initial-data-service
