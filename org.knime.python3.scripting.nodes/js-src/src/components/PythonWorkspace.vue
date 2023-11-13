<script setup lang="ts">
import { usePythonPreviewStatusStore, useWorkspaceStore } from "@/store";
import { getScriptingService } from "@knime/scripting-editor";
import { onMounted, ref } from "vue";
import Button from "webapps-common/ui/components/Button.vue";
import { pythonScriptingService } from "../python-scripting-service";

const workspaceStore = useWorkspaceStore();

const resetButtonEnabled = ref<boolean>(false);
const pythonPreviewStatus = usePythonPreviewStatusStore();

const resetWorkspace = () => {
  pythonScriptingService.killInteractivePythonSession();

  // reset view
  pythonPreviewStatus.clearView();
};

onMounted(async () => {
  if (await getScriptingService().inputsAvailable()) {
    resetButtonEnabled.value = true;
  }
});
</script>

<template>
  <div class="workspace">
    <table aria-label="Current python workspace variables.">
      <thead>
        <th>Name</th>
        <th>Type</th>
        <th>Value</th>
      </thead>
      <tbody>
        <tr
          v-for="variable in workspaceStore.workspace"
          :key="variable.name"
          @click="pythonScriptingService.printVariable(variable.name)"
        >
          <td>{{ variable.name }}</td>
          <td>{{ variable.type }}</td>
          <td>{{ variable.value }}</td>
        </tr>
      </tbody>
    </table>
    <div class="controls">
      <Button
        class="reset-button"
        with-border
        compact
        :disabled="!resetButtonEnabled"
        @click="resetWorkspace"
      >
        Reset temporary values
      </Button>
    </div>
  </div>
</template>

<style scoped lang="postcss">
.workspace {
  --controls-height: 40px;

  position: relative;
  width: 100%;
  min-width: 120px;
  height: 100%;

  & table {
    width: 100%;
    height: calc(100% - var(--controls-height));
    border-collapse: collapse;
    font-size: 12px;
    line-height: 24px;
  }
}

.controls {
  display: flex;
  flex-direction: row-reverse;
  justify-content: space-between;
  align-content: center;
  min-height: var(--controls-height);
  max-height: var(--controls-height);
  background-color: var(--knime-gray-light-semi);
  border-top: 1px solid var(--knime-silver-sand);
}

thead {
  background-color: var(--knime-porcelain);
  font-family: Roboto, sans-serif;
  position: sticky;
  height: 24px;
  top: 0;
  display: table;
  width: 100%;
  table-layout: fixed;
  backface-visibility: hidden;
}

tr {
  border-bottom: 1px solid var(--knime-porcelain);
  display: table;
  width: 100%;
  table-layout: fixed;

  &:hover {
    background-color: var(--knime-cornflower-semi);
    cursor: pointer;
  }
}

th,
td {
  overflow: hidden;
  white-space: nowrap;
  text-overflow: ellipsis;
  padding: 6px;
}

tbody {
  font-family: "Roboto Mono", sans-serif;
  display: block;
  overflow-y: scroll;
  overflow-x: hidden;
  text-overflow: ellipsis;
  height: calc(100% - var(--controls-height) + 4px);
}

th {
  text-align: left;
}

.reset-button {
  height: 30px;
  margin-top: 5px;
  margin-bottom: 5px;
  margin-right: 10px;
}
</style>
