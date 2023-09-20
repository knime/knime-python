<script setup lang="ts">
import { useWorkspaceStore } from "@/store";
import { getScriptingService } from "@knime/scripting-editor";
import { onMounted, ref } from "vue";
import Button from "webapps-common/ui/components/Button.vue";
import { pythonScriptingService } from "../python-scripting-service";

const workspaceStore = useWorkspaceStore();

const resetButtonEnabled = ref<boolean>(false);

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
    <Button
      class="reset-button"
      compact
      :disabled="!resetButtonEnabled"
      :with-border="false"
      @click="pythonScriptingService.killInteractivePythonSession"
    >
      Reset temporary values
    </Button>
  </div>
</template>

<style scoped lang="postcss">
.workspace {
  position: relative;
  width: 100%;
  min-width: 120px;
  height: 100%;

  & table {
    width: 100%;
    height: calc(100% - 50px);
    border-collapse: collapse;
    font-size: 12px;
    line-height: 24px;
  }
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
  height: calc(100% - 30px);
}

th {
  text-align: left;
}

.reset-button {
  min-width: 80px;
  height: 25px;
  margin: 0;
  position: absolute;
  bottom: 10px;
  right: 0;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
