<script setup lang="ts">
import { ScriptingEditor } from "@knime/scripting-editor";
import SettingsIcon from "webapps-common/ui/assets/img/icons/cog.svg";
import HelpIcon from "webapps-common/ui/assets/img/icons/help.svg";
import type { MenuItem } from "webapps-common/ui/components/MenuItems.vue";
import PythonEditorControls from "./PythonEditorControls.vue";
import PythonWorkspace from "./PythonWorkspace.vue";
import { pythonScriptingService } from "@/python-scripting-service";

const menuItems: MenuItem[] = [
  {
    // href: "https://docs.knime.com",
    text: "Python Editor setting 1",
    icon: HelpIcon,
  },
  {
    text: "Python Editor setting 2",
    icon: SettingsIcon,
  },
  {
    text: "More elements with long text",
    icon: SettingsIcon,
  },
];

const onMonacoCreated = () => {
  pythonScriptingService.connectToLanguageServer();
};
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
    >
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