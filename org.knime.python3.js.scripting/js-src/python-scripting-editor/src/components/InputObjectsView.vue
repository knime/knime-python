<script lang="ts">
import { InputObjects } from '../utils/python-scripting-service';
import Vue, { PropType } from 'vue';
import Button from '~/webapps-common/ui/components/Button.vue';

// TODO(AP-19346) use knime-ui-table or a tree view
// TODO(AP-19346) make this more general and move to knime-scripting-editor

export default Vue.extend({
    name: 'InputObjectsView',
    components: { Button },
    props: {
        inputObjects: {
            type: Array as PropType<InputObjects>,
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
      v-for="(table, index) in inputObjects"
      :key="index"
    >
      knio.input_tables[{{ index }}]
      <ul>
        <li
          v-for="column in table"
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
