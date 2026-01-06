<script setup lang="ts">
import { computed } from "vue";

import { useWorkspaceStore } from "@/store";
import { pythonScriptingService } from "../python-scripting-service";

import type { ColumnSizes } from "./PythonWorkspaceHeader.vue";

const workspaceStore = useWorkspaceStore();

interface Props {
  columnWidths: ColumnSizes;
}
const props = defineProps<Props>();

const totalWidth = computed(() =>
  props.columnWidths.reduce((a, c) => a + c, 0),
);
</script>

<template>
  <tbody>
    <tr
      v-for="variable in workspaceStore?.workspace"
      :key="variable.name"
      title="Print to console"
      :style="{ width: totalWidth + 'px' }"
      @click="pythonScriptingService.printVariable(variable.name)"
    >
      <td :style="{ width: columnWidths[0] + 'px' }">
        <span>{{ variable.name }}</span>
      </td>
      <td :style="{ width: columnWidths[1] + 'px' }">
        <span>{{ variable.type }}</span>
      </td>
      <td :style="{ width: columnWidths[2] + 'px' }">
        <span>{{ variable.value }}</span>
      </td>
    </tr>
  </tbody>
</template>

<style scoped lang="postcss">
tbody {
  display: table;

  & tr {
    display: flex;

    &:hover {
      color: var(--knime-masala);
      cursor: pointer;
      background-color: var(--kds-color-background-neutral-hover);
      border-radius: var(--kds-border-radius-container-0-25x);
    }

    & td {
      gap: var(--kds-spacing-container-0-12x);
      height: var(--kds-dimension-component-height-1-5x);
      padding-right: var(--kds-spacing-container-0-25x);
      padding-left: var(--kds-spacing-container-0-25x);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;

      & span {
        width: inherit;
        overflow: hidden;
        text-overflow: ellipsis;
        font: var(--kds-font-base-interactive-small);
        color: var(--kds-color-text-and-icon-neutral);
      }
    }
  }
}
</style>
