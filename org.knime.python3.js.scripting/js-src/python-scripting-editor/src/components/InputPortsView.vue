<script lang="ts">
import type { InputPortInfo } from '../utils/python-scripting-service';
import type { PropType } from 'vue';
import { defineComponent } from 'vue';
import Button from 'webapps-common/ui/components/Button.vue';

// TODO(AP-19346) use knime-ui-table or a tree view
// TODO(AP-19346) make this more general and move to knime-scripting-editor

export default defineComponent({
    name: 'InputObjectsView',
    components: { Button },
    props: {
        inputPortInfos: {
            type: Array as PropType<InputPortInfo[]>,
            default() {
                return [];
            }
        }
    }
});
</script>

<template>
  <ul>
    <li
      v-for="(port, index) in inputPortInfos"
      :key="index"
    >
      {{ port.variableName }}
      <ul v-if="port.type === 'table'">
        <li
          v-for="column in port.columnNames"
          :key="column"
        >
          {{ column }}
          <Button
            compact
            with-border
            @click="$emit('column-clicked', column)"
          >
            insert
          </Button>
        </li>
      </ul>
    </li>
  </ul>
</template>
