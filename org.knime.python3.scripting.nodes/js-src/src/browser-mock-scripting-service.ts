/* eslint-disable no-console */
import {
  EditorService,
  type NodeSettings,
  type ScriptingServiceType,
} from "@knime/scripting-editor";
import sleep from "webapps-common/util/sleep";

const SLEEP_TIME = 100;

const editorService = new EditorService();

const eventHandlers = new Map<string, (args: any) => void>();

const INPUT_OBJECTS = [
  {
    name: "Input Table 1",
    codeAlias: "knio.input_tables[0].to_pandas()",
    requiredImport: "import knio.scripting.io as knio",
    multiSelection: true,
    subItemCodeAliasTemplate: `knio.input_tables[0][
        {{~#if subItems.[1]~}}
          [{{#each subItems}}"{{this}}"{{#unless @last}},{{/unless}}{{/each}}]
        {{~else~}}
          "{{subItems.[0]}}"
          {{~/if~}}
      ].to_pandas()`,
    subItems: [
      {
        name: "Column 1",
        type: "Number",
      },
      {
        name: "Column 2",
        type: "String",
      },
      {
        name: "Column 3",
        type: "String",
      },
    ],
  },
  {
    name: "Input Table 2",
    codeAlias: "knio.input_tables[1].to_pandas()",
    requiredImport: "import knio.scripting.io as knio",
    multiSelection: true,
    subItemCodeAliasTemplate: `knio.input_tables[1][
        {{~#if subItems.[1]~}}
          [{{#each subItems}}"{{this}}"{{#unless @last}},{{/unless}}{{/each}}]
        {{~else~}}
          "{{subItems.[0]}}"
          {{~/if~}}
      ].to_pandas()`,
    subItems: [
      {
        name: "Column 1",
        type: "Number",
      },
      {
        name: "Column 2",
        type: "String",
      },
      {
        name: "Column 3",
        type: "String",
      },
    ],
  },
];
const OUTPUT_OBJECTS = [
  {
    name: "Output Table 1",
    codeAlias: "knio.output_tables[0]",
    requiredImport: "import knio.scripting.io as knio",
  },
];
const FLOW_VARIABLE_INPUTS = {
  name: "Flow Variables",
  codeAlias: "knio.flow_variables",
  requiredImport: "import knio.scripting.io as knio",
  multiSelection: false,
  subItemCodeAliasTemplate: 'knio.flow_variables["{{subItems.[0]}}"]',
  subItems: [
    {
      name: "flowVar1",
      type: "Number",
    },
    {
      name: "flowVar2",
      type: "String",
    },
    {
      name: "flowVar3",
      type: "String",
    },
  ],
};

const sendToServiceMockResponses = {
  suggestCode: async () => {
    await sleep(2000);
    const fn = eventHandlers.get("codeSuggestion");
    if (typeof fn !== "undefined") {
      fn({
        status: "SUCCESS",
        code: JSON.stringify({
          code: `import knime.scripting.io as knio

// this code does nothing yet
print("Hello, I am a fake AI")
`,
        }),
      });
    }
    return {};
  },
  getLanguageServerConfig: () => JSON.stringify({}),
  hasPreview: () => true,
  getExecutableOptionsList: () => [],
};

const browserMockScriptingService: Partial<ScriptingServiceType> = {
  async sendToService(methodName: string, options?: any[]) {
    console.log(`Called KNIME ${methodName} with ${JSON.stringify(options)}`);
    await sleep(SLEEP_TIME);

    if (Object.keys(sendToServiceMockResponses).includes(methodName)) {
      // @ts-ignore
      return sendToServiceMockResponses[methodName]();
    } else {
      return {
        status: "SUCCESS",
        description: "mocked execution info",
        jsonFromExecution: null,
      };
    }
  },
  async getInitialSettings() {
    await sleep(SLEEP_TIME);
    return {
      script:
        "print('Hello World!')\n\nprint('Hello World!')\n\nprint('Hello World!')\n",
    };
  },
  async saveSettings(settings: NodeSettings) {
    console.log(`Saved settings ${JSON.stringify(settings)}`);
    await sleep(SLEEP_TIME);
  },
  registerEventHandler(type: string, handler: (args: any) => void) {
    eventHandlers.set(type, handler);
    console.log(`Registered event handler for ${type}`);
  },
  registerLanguageServerEventHandler() {
    console.log("Registered language server event handler");
  },
  registerConsoleEventHandler() {
    console.log("Registered console event handler");
  },
  stopEventPoller() {
    // do nothing
  },
  sendToConsole(text: { text: string }) {
    console.log(`sending text to console: ${text}`);
  },
  initEditorService(editor: any, editorModel: any) {
    editorService.initEditorService({ editor, editorModel });
    console.log(`initEditorService called with ${{ editor, editorModel }}`);
  },
  getScript() {
    console.log("getScript called", editorService.getScript());
    return editorService.getScript();
  },
  getSelectedLines() {
    console.log("getSelectedLines called");
    return editorService.getSelectedLines();
  },
  setScript(newScript: string) {
    console.log(`Setting new script ${newScript}`);
    editorService.setScript(newScript);
  },
  async connectToLanguageServer() {
    console.log("Connecting to Language Server");
    await sleep(SLEEP_TIME);
  },
  async configureLanguageServer(config: any) {
    console.log(`Configuring Language Server: ${config}`);
    await sleep(SLEEP_TIME);
  },
  pasteToEditor(text: string) {
    editorService.pasteToEditor(text);
  },
  isCodeAssistantEnabled() {
    console.log("Checking whether code assistance is enabled");
    return Promise.resolve(true);
  },
  isCodeAssistantInstalled() {
    console.log("Checking whether code assistance is installed");
    return Promise.resolve(true);
  },
  inputsAvailable(): Promise<boolean> {
    console.log("Checking whether inputs are available");
    return Promise.resolve(true);
  },
  closeDialog() {
    console.log("Closing dialog");
    return Promise.resolve();
  },
  getFlowVariableInputs() {
    return Promise.resolve(FLOW_VARIABLE_INPUTS);
  },
  getInputObjects() {
    return Promise.resolve(INPUT_OBJECTS);
  },
  getOutputObjects() {
    return Promise.resolve(OUTPUT_OBJECTS);
  },
  clearConsole() {
    console.log("clearConsole was called");
  },
};

export default browserMockScriptingService;
