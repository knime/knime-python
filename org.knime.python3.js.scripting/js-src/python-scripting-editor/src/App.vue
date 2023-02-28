<script lang="ts">
import { defineComponent } from 'vue';

import PythonScriptingEditor from './components/PythonScriptingEditor.vue';
import LoadingIcon from 'webapps-common/ui/components/LoadingIcon.vue';

export default defineComponent({
    name: 'App',
    components: {
        PythonScriptingEditor,
        LoadingIcon,
    },
});
</script>

<template>
  <Suspense>
    <PythonScriptingEditor />
    <template #fallback>
      <LoadingIcon class="loading-icon" />
    </template>
  </Suspense>
</template>

<style lang="postcss">
@import 'webapps-common/ui/css';

</style>

<style lang="postcss" scoped>
  .loading-icon {
    position: absolute;
    right: calc(50vw - 50px);
    top: calc(50vh - 50px);
    width: 100px;
    height: 100px;
    stroke-width: calc(32px / 24);
    stroke: var(--knime-masala);
  }
</style>
