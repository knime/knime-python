<script lang="ts">
import Vue from 'vue';
import RadioButtons from '~/webapps-common/ui/components/forms/RadioButtons.vue';
import { PythonScriptingService, ExecutableOption, ExecutableInfo } from '../utils/python-scripting-service';

// TODO(AP-19168) backend for setting and getting the conda environment
// TODO(AP-19349) nice frontend

export default Vue.extend({
    name: 'CondaEnvironment',
    components: {
        RadioButtons
    },
    inject: ['getScriptingService'],
    data() {
        return {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore type inference does not work!
            scriptingService: this.getScriptingService(),
            selected: '',
            executableOptions: [],
            executableInfo: null
        } as {
            scriptingService: PythonScriptingService;
            selected: string;
            executableOptions: ExecutableOption[];
            executableInfo: ExecutableInfo | null;
        };
    },
    computed: {
        executablePossibleSelections(): { id: string; text: string }[] {
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
    },
    async mounted() {
        this.selected = this.scriptingService.getExecutableSelection();
        this.executableOptions = await this.scriptingService.getExecutableOptions(this.selected);
        this.selectionChanged(this.selected);
    },
    methods: {
        selectionChanged(id: string) {
            // Update the node settings
            this.scriptingService.setExecutableSelection(id);

            // Notify the parent that the selection changed -> Restarts Python session
            this.$emit('executable-changed');

            // Get the packages of the selected environment
            this.executableInfo = null;
            this.scriptingService.getExecutableInfo(id).then((info: ExecutableInfo) => {
                if (this.selected === id) {
                    // Only set the packages if this environment is still selected
                    this.executableInfo = info;
                }
            });
        }
    }
});
</script>

<template>
  <div>
    <!-- TODO(AP-19349) use a combobox if there are too many options? -->
    <RadioButtons
      v-model="selected"
      alignment="vertical"
      :possible-values="executablePossibleSelections"
      @input="selectionChanged"
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
