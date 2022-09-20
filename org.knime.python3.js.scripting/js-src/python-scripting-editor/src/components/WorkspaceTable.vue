<script lang="ts">
import type { PropType } from 'vue';
import type { Workspace } from '../utils/python-scripting-service';
import { defineComponent } from 'vue';

// TODO(AP-19345) use knime-ui-table

export default defineComponent({
    name: 'WorkspaceTable',
    props: {
        workspace: {
            type: Object as PropType<Workspace>,
            default() {
                return {
                    names: ['no workspace'],
                    types: [''],
                    values: ['']
                };
            }
        }
    },
    emits: ['clicked'],
    computed: {
        workspaceTable() {
            const { names, types, values } = this.workspace;
            return names.map((n: string, i: number) => ({ name: n, type: types[i], value: values[i] }));
        }
    }
});
</script>

<template>
  <table class="workspace-table">
    <tr>
      <th>Name</th>
      <th>Type</th>
      <th>Value</th>
    </tr>
    <tr
      v-for="item in workspaceTable"
      :key="item.name"
      @click="$emit('clicked', item.name)"
    >
      <td>{{ item.name }}</td>
      <td>{{ item.type }}</td>
      <td>{{ item.value }}</td>
    </tr>
  </table>
</template>

<style scoped>
table,
th,
td {
    border-collapse: collapse;
    border: 1px solid;
}
</style>
