<script lang="ts">
import { defineComponent } from 'vue';
import type { editor,
    Selection } from 'monaco-editor';
import Button from 'webapps-common/ui/components/Button.vue';
import LoadingIcon from 'webapps-common/ui/assets/img/icons/reload.svg'; // TODO(AP-19344) switch to LoadingIcon for the animation
import ScriptingEditor from 'scripting-editor/src/components/ScriptingEditor.vue';
import OutputConsole from 'scripting-editor/src/components/OutputConsole.vue';
import WorkspaceTable from './components/WorkspaceTable.vue';
import InputPortsView from './components/InputPortsView.vue';
import CondaEnvironment from './components/CondaEnvironment.vue';
import { createScriptingService, PythonScriptingService } from './utils/python-scripting-service';
import type { Workspace,
    InputPortInfo,
    ExecutableOption,
    ExecutableInfo,
    PythonNodeSettings } from './utils/python-scripting-service';

import { registerMonacoInputColumnCompletions } from './utils/python-completions';

const getOrThrow = <T, >(obj: T | undefined, name: string) => {
    if (obj) {
        return obj;
    } else {
        throw Error(`The ${name} is not yet defined. This is an implementation error.`);
    }
};

const getSelectedLines = (editorModel: editor.ITextModel, selection: Selection) => {
    const { startLineNumber, endLineNumber, endColumn } =
        selection.selectionStartLineNumber <= selection.positionLineNumber
            ? {
                // Selection goes from top to bottom
                startLineNumber: selection.selectionStartLineNumber,
                endLineNumber: selection.positionLineNumber,
                endColumn: selection.positionColumn
            }
            : {
                // Selection goes from bottom to top
                startLineNumber: selection.positionLineNumber,
                endLineNumber: selection.selectionStartLineNumber,
                endColumn: selection.selectionStartColumn
            };
    return editorModel.getValueInRange({
        startLineNumber,
        endLineNumber: endLineNumber + (endColumn > 1 ? 1 : 0),
        startColumn: 0,
        endColumn: 0
    });
};

type AppData = {
    scriptingService: PythonScriptingService;
    scriptingSettings?: PythonNodeSettings;
    loading: boolean;

    // Monaco editor
    editor?: editor.IStandaloneCodeEditor;
    editorModel?: editor.ITextModel;
    hasEditorSelection: boolean;

    // Python process handling
    pythonExecutableId: string;
    pythonExecutableOptions: ExecutableOption[];
    pythonExecutableInfo: ExecutableInfo | null;
    status: 'idle' | 'running';
    workspace: Workspace;

    // Script inputs
    inputPortInfos: InputPortInfo[];
};

export default defineComponent({
    name: 'App',
    components: {
        ScriptingEditor,
        Button,
        LoadingIcon,
        WorkspaceTable,
        InputPortsView,
        CondaEnvironment,
        OutputConsole
    },
    provide(): { getScriptingService: () => PythonScriptingService } {
        return { getScriptingService: () => this.getScriptingService() };
    },
    data(): AppData {
        return {
            loading: true,
            hasEditorSelection: false,
            scriptingService: null,
            // Python process handling
            pythonExecutableId: '',
            pythonExecutableOptions: [],
            pythonExecutableInfo: null,
            status: 'idle',
            workspace: {
                names: ['no workspace'],
                types: [''],
                values: ['']
            },

            // Script inputs
            inputPortInfos: []
        };
    },
    async mounted() {
        this.scriptingService = await createScriptingService();
        this.loading = false;

        // Notify the backend that the dialog is ready
        this.scriptingService.dialogOpened();

        // Set the python executable to the currently selected option -> will start the interactive session
        this.pythonExecutableChanged(this.scriptingService.getExecutableSelection());

        // Get some more information from the backend
        this.pythonExecutableOptions = await this.scriptingService.getExecutableOptions(this.pythonExecutableId);
        this.inputPortInfos = await this.getScriptingService().getInputObjects();

        // Add special autocompletion
        registerMonacoInputColumnCompletions(this.inputPortInfos);
    },
    methods: {
        startInteractive() {
            // TODO(AP-19332) handle gracefully if starting the interactive session fails
            this.getScriptingService().startInteractive(this.getScriptingService().getExecutableSelection());
        },
        /**
         * Select another executable option. Updates the state, restarts the interactive Python session and requests
         * information about the selected executable.
         *Select another executable option.
         * @param {id} id is the identifier of the selected python executable
         * @returns {None}
         */
        pythonExecutableChanged(id: string) {
            // Update the state
            this.pythonExecutableId = id;

            // Update the node settings
            this.getScriptingService().setExecutableSelection(id);

            // Restart the interactive Python process
            this.startInteractive();

            // Get the packages of the selected environment
            this.pythonExecutableInfo = null;
            this.getScriptingService().getExecutableInfo(id).then((info: ExecutableInfo) => {
                if (this.pythonExecutableId === id) {
                    // Only set the packages if this environment is still selected
                    this.pythonExecutableInfo = info;
                }
            });
        },
        variableClicked(event: string) {
            this.runScript(`print('print>' + '\\n' + str(${event}))`);
        },
        runSelectedLines() {
            const selection = this.getEditor().getSelection();
            if (selection) {
                this.runScript(getSelectedLines(this.getEditorModel(), selection));
            } else {
                // NB: This cannot happen because getSelection never returns null
                // and the button is disabled if there is no selection
                throw new Error('Inconsistent state. The editor has no selection.');
            }
        },
        runFullScript() {
            this.runScript(this.getEditorModel().getValue());
        },
        runScript(script: string) {
            this.status = 'running';
            this.getScriptingService()
                .runInteractive(script)
                .then((res: string) => {
                    this.workspace = JSON.parse(res);
                    this.status = 'idle';
                });
        },
        onMonacoCreated({
            editor,
            editorModel
        }: {
            editor: editor.IStandaloneCodeEditor;
            editorModel: editor.ITextModel;
        }) {
            this.editor = editor;
            this.editorModel = editorModel;

            // Always convert tabs of pasted code to spaces
            editor.onDidPaste(() => {
                editor.getAction('editor.action.indentationToSpaces').run();
            });

            // Start the language server client for Python
            this.getScriptingService().startLanguageClient('Python LSP', ['python']);

            // Enable the "Run selected lines" button only if there is a selection
            editor.onDidChangeCursorSelection((e) => {
                const s = e.selection;
                this.hasEditorSelection = !(
                    s.selectionStartLineNumber === s.positionLineNumber && s.selectionStartColumn === s.positionColumn
                );
            });
        },
        onColumnClicked(column: string) {
            // TODO(AP-19346) find out if the curser is inside quotes
            // if not: Add quotes, else: don't add quotes
            this.getEditor().trigger('keyboard', 'type', { text: column });
        },

        // Null-safe access to editor and editor model
        getScriptingService(): PythonScriptingService {
            // TODO(review) is this the right way to make the scripting service available?
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore type inference does not work!
            return getOrThrow(this.scriptingService, 'scripting service');
        },
        getEditor(): editor.IStandaloneCodeEditor {
            return getOrThrow(this.editor, 'editor');
        },
        getEditorModel(): editor.ITextModel {
            return getOrThrow(this.editorModel, 'editor model');
        }
    }
});
</script>

<template>
  <!-- TODO move loading animation into mixin -->
  <div
    v-if="loading"
    class="loading"
  >
    <LoadingIcon class="loading-icon" />
  </div>
  <div
    v-else
    id="app"
  >
    <ScriptingEditor
      language="python"
      initial-left-tab="inputs"
      :left-tabs="[
        { value: 'inputs', label: 'Inputs' },
        { value: 'conda_env', label: 'Conda Environment' }
      ]"
      initial-bottom-tab="console"
      :bottom-tabs="[{ value: 'console', label: 'Console' }]"
      initial-right-tab="workspace"
      :right-tabs="[{ value: 'workspace', label: 'Workspace' }]"
      @monaco-created="onMonacoCreated"
    >
      <template #buttons>
        <!-- TODO(AP-19344) extract into a dumb component to cleanup the template -->
        <Button
          with-border
          compact
          @click="runFullScript"
        >
          Run script
        </Button>
        <Button
          with-border
          compact
          :disabled="!hasEditorSelection"
          @click="runSelectedLines"
        >
          Run selected lines
        </Button>
        {{ status }}
      </template>

      <template #lefttabs="{ activeTab }">
        <InputPortsView
          v-if="activeTab === 'inputs'"
          :input-port-infos="inputPortInfos"
          @column-clicked="onColumnClicked"
        />
        <CondaEnvironment
          v-if="activeTab === 'conda_env'"
          :value="pythonExecutableId"
          :executable-options="pythonExecutableOptions"
          :executable-info="pythonExecutableInfo"
          @input="pythonExecutableChanged"
        />
      </template>

      <template #bottomtabs="{ activeTab }">
        <keep-alive>
          <OutputConsole v-if="activeTab === 'console'" />
        </keep-alive>
      </template>

      <template #righttabs="{ activeTab }">
        <WorkspaceTable
          v-if="activeTab === 'workspace'"
          :workspace="workspace"
          @clicked="variableClicked"
        />
      </template>
    </ScriptingEditor>
  </div>
</template>

<style lang="postcss">
@import 'webapps-common/ui/css';

:root {
    font-size: 16px;
    line-height: 1.44;
}
</style>

<style type="postcss" scoped>
/* TODO(AP-19344) remove and use LoadingIcon.vue */
@keyframes spin {
    100% {
        transform: rotate(-360deg);
    }
}

.loading {
    display: flex;
    align-items: center;
    justify-content: center;
    height: 100vh;

    & .loading-icon {
        width: 32px;
        stroke: var(--knime-masala);
        /* TODO(AP-19344) remove and use LoadingIcon.vue */
        animation: spin 2s linear infinite;
    }
}
</style>
