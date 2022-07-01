<script lang="ts">
import Vue, { PropType } from 'vue';
import { Workspace } from '../utils/python-scripting-service';

// TODO(AP-19345) use knime-ui-table

export default Vue.extend({
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
