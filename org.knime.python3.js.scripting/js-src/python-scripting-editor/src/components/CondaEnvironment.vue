<script lang="ts">
import type { PropType } from 'vue';
import { defineComponent } from 'vue';
import RadioButtons from 'webapps-common/ui/components/forms/RadioButtons.vue';
import type { ExecutableOption, ExecutableInfo } from '../utils/python-scripting-service';

// TODO(AP-19168) backend for setting and getting the conda environment
// TODO(AP-19349) nice frontend

export default defineComponent({
    name: 'CondaEnvironment',
    components: {
        RadioButtons
    },
    props: {
        value: {
            type: String,
            default: () => ''
        },
        executableOptions: {
            type: Array as () => Array<ExecutableOption>,
            default: () => []
        },
        executableInfo: {
            type: Object as PropType<ExecutableInfo>,
            default: () => null
        }
    },
    emits: ['input'],
    computed: {
        executablePossibleSelections(): { id: string; text: string }[] {
            // TODO(AP-19349) Move this into its own component
            return this.executableOptions.map((option: ExecutableOption) => {
                switch (option.type) {
                    case 'PREF_BUNDLED':
                        return { id: option.id, text: 'Preferences (Bundled)' };

                    case 'PREF_CONDA':
                        return { id: option.id, text: 'Preferences (Conda)' };

                    case 'PREF_MANUAL':
                        return { id: option.id, text: 'Preferences (Manual)' };

                    case 'CONDA_ENV_VAR':
                        return { id: option.id, text: option.id };

                    case 'STRING_VAR':
                        return { id: option.id, text: `${option.id} (string)` };

                    case 'MISSING_VAR':
                        return { id: option.id, text: `${option.id} (missing)` };
                }
                // Cannot happen
                throw new Error(`Invalid executable option type: ${option.type}`);
            });
        }
    }
});
</script>

<template>
  <div>
    <!-- TODO(AP-19349) use a combobox if there are too many options? -->
    <RadioButtons
      :value="value"
      alignment="vertical"
      :possible-values="executablePossibleSelections"
      @input="$emit('input', $event)"
    />
    <div
      v-if="executableInfo"
      class="executable-info"
    >
      <span v-if="executableInfo.pythonVersion"> Python Version: {{ executableInfo.pythonVersion }} </span>
      <table>
        <tr>
          <th>Name</th>
          <th>Version</th>
          <th>Build</th>
          <th>Channel</th>
        </tr>
        <tr
          v-for="item in executableInfo.packages"
          :key="item.name"
        >
          <td>{{ item.name }}</td>
          <td>{{ item.version }}</td>
          <td>{{ item.build }}</td>
          <td>{{ item.channel }}</td>
        </tr>
      </table>
    </div>
    <div v-else>
      <!-- TODO(AP-19349) loading animation while we are getting the executable info -->
      Loading environment information...
    </div>
  </div>
</template>

<style scoped>
table,
th,
td {
    font-size: small;
    border-collapse: collapse;
    border: 1px solid;
}
</style>
