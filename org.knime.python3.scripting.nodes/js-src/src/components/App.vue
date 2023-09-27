<script setup lang="ts">
import { pythonScriptingService } from "@/python-scripting-service";
import { useExecutableSelectionStore } from "@/store";
import {
  ScriptingEditor,
  type NodeSettings,
  type SettingsMenuItem,
} from "@knime/scripting-editor";
import * as monaco from "monaco-editor";
import { onMounted, ref, type Ref } from "vue";
import SettingsIcon from "webapps-common/ui/assets/img/icons/cog.svg";
import HelpIcon from "webapps-common/ui/assets/img/icons/help.svg";
import type { MenuItem } from "webapps-common/ui/components/MenuItems.vue";
import TabBar from "webapps-common/ui/components/TabBar.vue";
import EnvironmentSettings from "./EnvironmentSettings.vue";
import PythonEditorControls from "./PythonEditorControls.vue";
import PythonViewPreview from "./PythonViewPreview.vue";
import PythonWorkspace from "./PythonWorkspace.vue";

const menuItems: MenuItem[] = [
  {
    text: "Set Python executable",
    icon: SettingsIcon,
    showSettingsPage: true,
    title: "Select Environment",
    separator: true,
  } as SettingsMenuItem,
  {
    text: "KNIME Python Integration Guide",
    icon: HelpIcon,
    href: "https://docs.knime.com/latest/python_installation_guide/index.html#_introduction",
  } as MenuItem,
  {
    text: "KNIME Python Script API Documentation",
    icon: HelpIcon,
    href: "https://knime-python.readthedocs.io/en/stable/script-api.html",
  } as MenuItem,
];

const currentSettingsMenuItem: Ref<SettingsMenuItem | null> = ref(null);

const onMenuItemClicked = ({ item }: { item: SettingsMenuItem }) => {
  currentSettingsMenuItem.value = item;
};

const onMonacoCreated = ({
  editor,
  editorModel,
}: {
  editor: monaco.editor.IStandaloneCodeEditor;
  editorModel: monaco.editor.ITextModel;
}) => {
  // Use 4 spaces instead of tabs
  editorModel.updateOptions({
    tabSize: 4,
    insertSpaces: true,
  });

  // Replace tabs by spaces on paste
  editor.onDidPaste(() => {
    editor.getAction("editor.action.indentationToSpaces")?.run();
  });

  // Connect to the language server
  pythonScriptingService.connectToLanguageServer();
};

const saveSettings = async (commonSettings: NodeSettings) => {
  const settings = {
    ...commonSettings,
    executableSelection: useExecutableSelectionStore().id,
  };
  await pythonScriptingService.saveSettings(settings);
  pythonScriptingService.closeDialog();
};

// Right pane tab bar - only show if preview is available
const hasPreview = ref<boolean>(false);
type RightPaneTabValue = "workspace" | "preview";
// NB: The TabBar always selects the first enabled tab on create -> We cannot select the preview
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
});
</script>

<template>
  <main>
    <ScriptingEditor
      title="Python Script (Labs)"
      :menu-items="menuItems"
      :show-run-buttons="true"
      language="python"
      file-name="main.py"
      @monaco-created="onMonacoCreated"
      @menu-item-clicked="onMenuItemClicked"
      @save-settings="saveSettings"
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
          <TabBar
            v-model="rightPaneActiveTab"
            :possible-values="rightPaneOptions"
            :disabled="false"
          />
          <div id="right-pane-content">
            <PythonWorkspace v-show="rightPaneActiveTab === 'workspace'" />
            <PythonViewPreview v-show="rightPaneActiveTab === 'preview'" />
          </div>
        </div>
        <PythonWorkspace v-if="!hasPreview" />
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
