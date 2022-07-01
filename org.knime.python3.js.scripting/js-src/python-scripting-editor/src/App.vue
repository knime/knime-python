<script lang="ts">
import Vue from 'vue';
import * as monaco from 'monaco-editor';
import Button from '~/webapps-common/ui/components/Button.vue';
import ScriptingEditor from 'scripting-editor/src/components/ScriptingEditor.vue';
import OutputConsole from 'scripting-editor/src/components/OutputConsole.vue';
import WorkspaceTable from './components/WorkspaceTable.vue';
import InputObjectsView from './components/InputObjectsView.vue';
import CondaEnvironment from './components/CondaEnvironment.vue';
import { createScriptingService,
    PythonScriptingService,
    Workspace,
    InputObjects } from './utils/python-scripting-service';
import { registerMonacoInputColumnCompletions } from './utils/python-completions';

const getOrThrow = <T, >(obj: T | undefined, name: string) => {
    if (obj) {
        return obj;
    } else {
        throw Error(`The ${name} is not yet defined. This is an implementation error.`);
    }
};

export default Vue.extend({
    name: 'App',
    components: {
        ScriptingEditor,
        Button,
        WorkspaceTable,
        InputObjectsView,
        CondaEnvironment,
        OutputConsole
    },
    provide(): { getScriptingService: () => PythonScriptingService } {
        return { getScriptingService: () => this.getScriptingService() };
    },
    data() {
        return {
            loading: true,
            status: 'idle',
            inputObjects: [],
            hasEditorSelection: false,
            workspace: {
                names: ['no workspace'],
                types: [''],
                values: ['']
            }
        } as {
            loading: boolean;
            scriptingService?: PythonScriptingService;
            editor?: monaco.editor.IStandaloneCodeEditor;
            editorModel?: monaco.editor.ITextModel;
            status: 'idle' | 'running';
            hasEditorSelection: boolean;
            inputObjects: InputObjects;
            workspace: Workspace;
        };
    },
    async mounted() {
        this.scriptingService = await createScriptingService();
        this.loading = false;
        this.getScriptingService().startInteractive();
        this.inputObjects = await this.getScriptingService().getInputObjects();
        registerMonacoInputColumnCompletions(this.inputObjects);
    },
    methods: {
        runSelectedLines() {
            const selection = this.getEditor().getSelection();
            if (selection) {
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
                const script = this.getEditorModel().getValueInRange({
                    startLineNumber,
                    endLineNumber: endLineNumber + (endColumn > 1 ? 1 : 0),
                    startColumn: 0,
                    endColumn: 0
                });
                this.runScript(script);
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
            editor: monaco.editor.IStandaloneCodeEditor;
            editorModel: monaco.editor.ITextModel;
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
            return getOrThrow(this.scriptingService, 'scripting service');
        },
        getEditor(): monaco.editor.IStandaloneCodeEditor {
            return getOrThrow(this.editor, 'editor');
        },
        getEditorModel(): monaco.editor.ITextModel {
            return getOrThrow(this.editorModel, 'editor model');
        }
    }
});
</script>

<template>
  <div v-if="loading">
    <!-- TODO(AP-19344) loading animation -->
    Loading...
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
        <InputObjectsView
          v-if="activeTab === 'inputs'"
          :input-objects="inputObjects"
          @column-clicked="onColumnClicked"
        />
        <CondaEnvironment v-if="activeTab === 'conda_env'" />
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
        />
      </template>
    </ScriptingEditor>
  </div>
</template>

<style>
@import 'webapps-common/ui/css';

:root {
    font-size: 16px;
    line-height: 1.44;
}
</style>
