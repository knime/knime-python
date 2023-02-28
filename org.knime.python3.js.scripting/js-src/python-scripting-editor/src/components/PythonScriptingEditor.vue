<script lang="ts">

import { defineComponent, KeepAlive } from 'vue';
import { createScriptingService } from '@/utils/python-scripting-service';

import type { editor,
    Selection } from 'monaco-editor';
import Button from 'webapps-common/ui/components/Button.vue';
import ScriptingEditor from 'scripting-editor/src/components/ScriptingEditor.vue';
import OutputConsole from 'scripting-editor/src/components/OutputConsole.vue';
import WorkspaceTable from '../components/WorkspaceTable.vue';
import InputPortsView from '../components/InputPortsView.vue';
import CondaEnvironment from '../components/CondaEnvironment.vue';

import PlayIcon from 'webapps-common/ui/assets/img/icons/play.svg';

import type { Workspace,
    InputPortInfo,
    ExecutableOption,
    ExecutableInfo,
    PythonNodeSettings } from '../utils/python-scripting-service';

import FlowVariables from 'scripting-editor/src/components/FlowVariables.vue';
import type { FlowVariableInput } from 'scripting-editor/src/utils/scripting-service';
import { registerMonacoInputColumnCompletions,
    registerMonacoInputFlowVariableCompletions } from '../utils/python-completions';

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
    // Service
    scriptingSettings?: PythonNodeSettings;

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
    flowVariableInputs: FlowVariableInput[];
    inputPortInfos: InputPortInfo[];
};

export default defineComponent({
    name: 'PythonScriptingEditor',
    components: {
        ScriptingEditor,
        PlayIcon,
        Button,
        WorkspaceTable,
        InputPortsView,
        CondaEnvironment,
        KeepAlive,
        FlowVariables,
        OutputConsole
    },
    provide() {
        return {
            scriptingService: this.scriptingService
        };
    },
    async setup() {
        const scriptingService = await createScriptingService();
        return { scriptingService };
    },
    data(): AppData {
        return {
            // Monaco editor
            hasEditorSelection: false,
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
            inputPortInfos: [],
            flowVariableInputs: []
        };
    },
    async mounted() {
        // Notify the backend that the dialog is ready
        await this.scriptingService.initExecutableOptions();

        // Set the python executable to the currently selected option -> will start the interactive session
        this.pythonExecutableChanged(this.scriptingService.getExecutableSelection());

        // Get some more information from the backend
        this.pythonExecutableOptions = await this.scriptingService.getExecutableOptions(this.pythonExecutableId);
        this.inputPortInfos = await this.scriptingService.getInputObjects();
        this.flowVariableInputs = await this.scriptingService.getFlowVariableInputs();


        // Add special autocompletion
        // TODO (AP-20083): Abstraction into Python specific completions and scripting-service specific
        //      Could help to remove monaco dependency for the pythons-scripting-editor
        registerMonacoInputColumnCompletions(this.inputPortInfos);
        registerMonacoInputFlowVariableCompletions(this.flowVariableInputs);
    },
    methods: {
        startInteractive() {
            // TODO(AP-19332) handle gracefully if starting the interactive session fails
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            this.scriptingService.startInteractive(this.scriptingService.getExecutableSelection());
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
            this.scriptingService.setExecutableSelection(id);

            // Restart the interactive Python process
            this.startInteractive();

            // Get the packages of the selected environment
            this.pythonExecutableInfo = null;
            this.scriptingService.getExecutableInfo(id).then((info: ExecutableInfo) => {
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
            this.scriptingService.runInteractive(script)
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
            this.scriptingService.startLanguageClient('Python LSP', ['python']);
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
        getEditor(): editor.IStandaloneCodeEditor {
            return this.editor;
        },
        getEditorModel(): editor.ITextModel {
            return this.editorModel;
        }
    }
});

</script>

<template>
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
      <Button
        :title="'Run script'"
        with-border
        compact
        @click="runFullScript"
      >
        <PlayIcon />
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

    <template #inputs>
      <InputPortsView
        :input-port-infos="inputPortInfos"
        @column-clicked="onColumnClicked"
      />
      <FlowVariables
        :flow-variables="flowVariableInputs"
      />
    </template>

    <template #conda_env>
      <CondaEnvironment
        :value="pythonExecutableId"
        :executable-options="pythonExecutableOptions"
        :executable-info="pythonExecutableInfo"
        @input="pythonExecutableChanged"
      />
    </template>
    <template #bottomtabs="{ activeTab }">
      <KeepAlive>
        <OutputConsole v-if="activeTab === 'console'" />
      </KeepAlive>
    </template>

    <template #righttabs="{ activeTab }">
      <WorkspaceTable
        v-if="activeTab === 'workspace'"
        :workspace="workspace"
        @clicked="variableClicked"
      />
    </template>
  </ScriptingEditor>
</template>
