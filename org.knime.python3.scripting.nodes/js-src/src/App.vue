<script setup lang="ts">
import { editor } from "monaco-editor";
import { ScriptingEditor } from "@knime/scripting-editor";
import Button from "webapps-common/ui/components/Button.vue";
import SettingsIcon from "webapps-common/ui/assets/img/icons/cog.svg";
import HelpIcon from "webapps-common/ui/assets/img/icons/help.svg";

import { pythonScriptingService } from "@/python-scripting-service";

let editorModel: editor.ITextModel | null = null;

const onMonacoCreated = ({
  editorModel: newEditorModel,
}: {
  editor: editor.IStandaloneCodeEditor;
  editorModel: editor.ITextModel;
}) => {
  editorModel = newEditorModel;
};

const onSaveSettings = () => {
  if (editorModel) {
    pythonScriptingService.saveSettings({ script: editorModel.getValue() });
  }
};

const menuItems = [
  {
    // href: "https://docs.knime.com",
    text: "Help",
    icon: HelpIcon,
    separator: true,
  },
  {
    text: "Settings",
    icon: SettingsIcon,
  },
  {
    text: "Enlarge",
    icon: SettingsIcon,
  },
  {
    text: "Make it big",
    icon: HelpIcon,
  },
  {
    text: "More elements with long text",
    icon: SettingsIcon,
  },
];

</script>

<template>
  <main>
    <!-- TODO delete the save button when it is provided by the UI Extension framework -->
    <Button primary @click="onSaveSettings">Save Settings</Button>
    <ScriptingEditor
      title="KNIME Python Script Editor (JS)"
      :menu-items="menuItems"
      :show-run-buttons="true"
      language="python"
      file-name="main.py"
      @monaco-created="onMonacoCreated"
    />
  </main>
</template>

<style>
@import url("webapps-common/ui/css");
</style>
