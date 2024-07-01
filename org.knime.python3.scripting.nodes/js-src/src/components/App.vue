<script setup lang="ts">
import { pythonScriptingService } from "@/python-scripting-service";
import { useExecutableSelectionStore } from "@/store";
import {
  CompactTabBar,
  ScriptingEditor,
  editor,
  type NodeSettings,
  type SettingsMenuItem,
} from "@knime/scripting-editor";
import { nextTick, onMounted, ref, watch, type Ref } from "vue";
import SettingsIcon from "webapps-common/ui/assets/img/icons/cog.svg";
import FileCogIcon from "webapps-common/ui/assets/img/icons/file-cog.svg";
import FileTextIcon from "webapps-common/ui/assets/img/icons/file-text.svg";
import type { MenuItem } from "webapps-common/ui/components/MenuItems.vue";
import EnvironmentSettings from "./EnvironmentSettings.vue";
import LastActionStatus from "./LastActionStatus.vue";
import PythonEditorControls from "./PythonEditorControls.vue";
import PythonViewPreview from "./PythonViewPreview.vue";
import PythonWorkspace from "./PythonWorkspace.vue";

const menuItems: MenuItem[] = [
  {
    text: "Set Python environment",
    icon: SettingsIcon,
    showSettingsPage: true,
    title: "Select Environment",
    separator: true,
  } as SettingsMenuItem,
  {
    text: "KNIME Documentation",
    icon: FileTextIcon,
    href: "https://docs.knime.com/latest/python_installation_guide/index.html#_introduction",
  } as MenuItem,
  {
    text: "KNIME Python Script API",
    icon: FileCogIcon,
    href: "https://knime-python.readthedocs.io/en/stable/script-api.html",
  } as MenuItem,
];

const currentSettingsMenuItem: Ref<SettingsMenuItem | null> = ref(null);

const onMenuItemClicked = ({ item }: { item: SettingsMenuItem }) => {
  currentSettingsMenuItem.value = item;
};

const mainEditorState = editor.useMainCodeEditorStore();

// Use 4 spaces instead of tabs
watch(
  () => mainEditorState.value?.editorModel,
  (newEditorModel) => {
    newEditorModel?.updateOptions({
      tabSize: 4,
      insertSpaces: true,
    });
  },
);

// Replace tabs by spaces on paste
watch(
  () => mainEditorState.value?.editor.value,
  (newEditor) => {
    newEditor?.onDidPaste(() => {
      newEditor?.getAction("editor.action.indentationToSpaces")?.run();
    });
  },
);

// Connect to the language server
watch(
  () => mainEditorState.value?.editorModel,
  () => pythonScriptingService.connectToLanguageServer(),
);

// Register the completion items for the inputs
pythonScriptingService.registerInputCompletions();

const executableSelection = useExecutableSelectionStore();
const toSettings = (commonSettings: NodeSettings) => ({
  ...commonSettings,
  executableSelection:
    executableSelection.livePrefValue ?? executableSelection.id,
});

// Right pane tab bar - only show if preview is available
const hasPreview = ref<boolean>(false);
type RightPaneTabValue = "workspace" | "preview";
const rightPaneActiveTab = ref<RightPaneTabValue>("workspace");
const rightPaneOptions = [
  { value: "workspace", label: "Temporary Values" },
  { value: "preview", label: "Output Preview" },
];

onMounted(async () => {
  pythonScriptingService.sendLastConsoleOutput();

  pythonScriptingService.initExecutableSelection();

  // Check if the preview is available
  hasPreview.value = await pythonScriptingService.hasPreview();

  if (hasPreview.value) {
    // wait until preview is mounted to DOM
    await nextTick();
    rightPaneActiveTab.value = "preview";
  }
});
</script>

<template>
  <main>
    <ScriptingEditor
      :title="`Python ${hasPreview ? 'View' : 'Script'}`"
      :menu-items="menuItems"
      :show-run-buttons="true"
      :to-settings="toSettings"
      language="python"
      file-name="main.py"
      @menu-item-clicked="onMenuItemClicked"
    >
      <template #settings-title>
        {{ currentSettingsMenuItem?.title }}
      </template>
      <template #settings-content>
        <EnvironmentSettings
          v-if="currentSettingsMenuItem?.title === 'Select Environment'"
        />
      </template>
      <template #code-editor-controls>
        <PythonEditorControls />
      </template>
      <template #right-pane>
        <div v-if="hasPreview" id="right-pane">
          <CompactTabBar
            ref="rightTabBar"
            v-model="rightPaneActiveTab"
            :possible-values="rightPaneOptions"
            :disabled="false"
            name="rightTabBar"
          />
          <div id="right-pane-content">
            <PythonWorkspace v-show="rightPaneActiveTab === 'workspace'" />
            <PythonViewPreview v-show="rightPaneActiveTab === 'preview'" />
          </div>
        </div>
        <PythonWorkspace v-if="!hasPreview" />
      </template>
      <template #console-status>
        <LastActionStatus />
      </template>
    </ScriptingEditor>
  </main>
</template>

<style>
@import url("webapps-common/ui/css");
</style>

<style scoped lang="postcss">
#right-pane {
  height: 100%;
  display: flex;
  flex-direction: column;
}

#right-pane-content {
  flex-grow: 1;
}
</style>
