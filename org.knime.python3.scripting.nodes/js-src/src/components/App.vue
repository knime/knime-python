<script setup lang="ts">
import {
  ScriptingEditor,
  type NodeSettings,
  type SettingsMenuItem,
} from "@knime/scripting-editor";
import SettingsIcon from "webapps-common/ui/assets/img/icons/cog.svg";
import type { MenuItem } from "webapps-common/ui/components/MenuItems.vue";
import PythonEditorControls from "./PythonEditorControls.vue";
import PythonWorkspace from "./PythonWorkspace.vue";
import { pythonScriptingService } from "@/python-scripting-service";
import * as monaco from "monaco-editor";
import { onMounted, ref, type Ref } from "vue";
import EnvironmentSettings from "./EnvironmentSettings.vue";
import { useExecutableSelectionStore } from "@/store";

const menuItems: MenuItem[] = [
  {
    text: "Set python executable",
    icon: SettingsIcon,
    showSettingsPage: true,
    title: "Select Environment",
  } as SettingsMenuItem,
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

onMounted(() => {
  pythonScriptingService.initExecutableSelection();
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
        <PythonWorkspace />
      </template>
    </ScriptingEditor>
  </main>
</template>

<style>
@import url("webapps-common/ui/css");
</style>
