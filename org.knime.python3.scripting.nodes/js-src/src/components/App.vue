<script setup lang="ts">
import { type Ref, nextTick, onMounted, ref, watch } from "vue";
import * as monaco from "monaco-editor";

import { FunctionButton, LoadingIcon, type MenuItem } from "@knime/components";
import {
  CompactTabBar,
  type ConsoleHandler,
  type GenericNodeSettings,
  OutputConsole,
  ScriptingEditor,
  type SettingsMenuItem,
  consoleHandler,
  editor,
  initConsoleEventHandler,
  setConsoleHandler,
} from "@knime/scripting-editor";
import SettingsIcon from "@knime/styles/img/icons/cog.svg";
import FileCogIcon from "@knime/styles/img/icons/file-cog.svg";
import FileTextIcon from "@knime/styles/img/icons/file-text.svg";
import TrashIcon from "@knime/styles/img/icons/trash.svg";

import EnvironmentSettings from "@/components/EnvironmentSettings.vue";
import LastActionStatus from "@/components/LastActionStatus.vue";
import PythonEditorControls from "@/components/PythonEditorControls.vue";
import PythonViewPreview from "@/components/PythonViewPreview.vue";
import PythonWorkspace from "@/components/PythonWorkspace.vue";
import { getPythonInitialDataService } from "@/python-initial-data-service";
import { pythonScriptingService } from "@/python-scripting-service";
import { useExecutableSelectionStore } from "@/store";

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

watch(
  () => mainEditorState.value?.editor.value,
  (newEditor) => {
    // Replace tabs by spaces on paste
    newEditor?.onDidPaste(() => {
      newEditor?.getAction("editor.action.indentationToSpaces")?.run();
    });

    // Register respective commands to run script and selected lines
    newEditor?.addCommand(
      // eslint-disable-next-line no-bitwise
      monaco.KeyMod.Shift | monaco.KeyCode.Enter,
      () => {
        if (newEditor?.hasTextFocus()) {
          pythonScriptingService.runScript();
        }
      },
    );

    newEditor?.addCommand(
      // eslint-disable-next-line no-bitwise
      monaco.KeyMod.Shift | monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
      () => {
        if (newEditor?.hasTextFocus()) {
          pythonScriptingService.runSelectedLines();
        }
      },
    );
  },
);

// Connect to the language server
watch(
  () => mainEditorState.value?.editorModel,
  () => pythonScriptingService.connectToLanguageServer(),
);

const initialDataLoaded = ref(false);

// Register the completion items for the inputs
pythonScriptingService.registerInputCompletions();

const executableSelection = useExecutableSelectionStore();
const toSettings = (commonSettings: GenericNodeSettings) => ({
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

  await pythonScriptingService.initExecutableSelection();

  const initialData = await getPythonInitialDataService().getInitialData();

  initialDataLoaded.value = true;
  // Check if the preview is available
  hasPreview.value = initialData.hasPreview;

  if (hasPreview.value) {
    // wait until preview is mounted to DOM
    await nextTick();
    rightPaneActiveTab.value = "preview";
  }
});
</script>

<template>
  <main>
    <template v-if="initialDataLoaded">
      <ScriptingEditor
        :title="`Python ${hasPreview ? 'View' : 'Script'}`"
        language="python"
        file-name="main.py"
        :menu-items="menuItems"
        :to-settings="toSettings"
        :initial-pane-sizes="{
          left: 260,
          right: hasPreview ? 380 : 260,
          bottom: 300,
        }"
        :additional-bottom-pane-tab-content="[
          {
            slotName: 'bottomPaneTabSlot:console',
            label: 'Console',
            associatedControlsSlotName: 'bottomPaneTabControlsSlot:console',
          },
        ]"
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
        <template #bottom-pane-status-label>
          <LastActionStatus />
        </template>
        <template #bottomPaneTabSlot:console>
          <OutputConsole
            class="console"
            @console-created="
              (console: ConsoleHandler) => {
                setConsoleHandler(console);
                initConsoleEventHandler();
              }
            "
          />
        </template>
        <template #bottomPaneTabControlsSlot:console>
          <FunctionButton class="clear-button" @click="consoleHandler.clear">
            <TrashIcon />
          </FunctionButton>
        </template>
      </ScriptingEditor>
    </template>
    <template v-else>
      <div class="no-editors">
        <LoadingIcon />
      </div>
    </template>
  </main>
</template>

<style>
@import url("@knime/styles/css");
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

.no-editors {
  position: absolute;
  inset: calc(50% - 25px);
  width: 50px;
  height: 50px;
}
</style>
