<script setup lang="ts">
import { computed } from "vue";

import { useWorkspaceStore } from "@/store";
import { pythonScriptingService } from "../python-scripting-service";

import type { ColumnSizes } from "./PythonWorkspaceHeader.vue";

const workspaceStore = useWorkspaceStore();

interface Props {
  columnWidths: ColumnSizes;
}

const props = withDefaults(defineProps<Props>(), {
  columnWidths: () => [100, 100, 100],
});

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
  color: var(--knime-dove-gray);
  font-weight: 400;
  line-height: 18px;
  font-size: 13px;
  letter-spacing: 0;
  display: table;

  & tr {
    border-bottom: 1px solid var(--knime-porcelain);
    color: var(--knime-dove-gray);
    display: flex;

    &:hover {
      cursor: pointer;
      color: var(--knime-masala);
    }

    & td {
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;

      & span {
        width: inherit;
        margin: 6px;
        overflow: hidden;
        text-overflow: ellipsis;
      }
    }
  }
}
</style>
