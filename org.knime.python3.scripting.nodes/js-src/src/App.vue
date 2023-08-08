<script setup lang="ts">
import { editor } from "monaco-editor";
import { CodeEditor } from "@knime/scripting-editor";
import Button from "webapps-common/ui/components/Button.vue";

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
</script>

<template>
  <main>
    <div style="height: 600px">
      <CodeEditor language="python" @monaco-created="onMonacoCreated" />
    </div>
    <!-- TODO delete the save button when it is provided by the UI Extension framework -->
    <Button primary @click="onSaveSettings">Save Settings</Button>
  </main>
</template>

<style lang="postcss">
@import url("webapps-common/ui/css");
</style>
