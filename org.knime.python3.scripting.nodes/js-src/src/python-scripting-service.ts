import {
  type NodeSettings,
  type ScriptingServiceType,
  EditorService,
  getScriptingService,
} from "@knime/scripting-editor";

import {
  type ExecutableOption,
  type ExecutionInfo,
  type InputPortInfo,
  type KillSessionInfo,
  type PythonScriptingNodeSettings,
  type StartSessionInfo,
} from "./types/common";

import sleep from "webapps-common/util/sleep";
import { setSelectedExecutable, useExecutableSelectionStore } from "./store";

/* eslint-disable no-console */

const SLEEP_TIME = 500;

const editorService = new EditorService();

const eventHandlers = new Map<string, (args: any) => void>();

const sendToServiceMockResponses = {
  suggestCode: () => {
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
  getInputObjects: () => [
    {
      name: "Input Table 1",
      codeAlias: "knio.input_tables[0]",
      subItems: [
        {
          name: "Column 1",
          type: "Number",
          codeAlias: 'knio.input_tables[0]["Column 1"]',
        },
        {
          name: "Column 2",
          type: "String",
          codeAlias: 'knio.input_tables[0]["Column 2"]',
        },
      ],
    },
  ],
  getOutputObjects: () => [
    {
      name: "Output Table 1",
      codeAlias: "knio.output_tables[0]",
    },
  ],
  getFlowVariableInputs: () => {},
};

const browserMockScriptingService: ScriptingServiceType = {
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
    return { script: "print('Hello World!')" };
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
    console.log("getSelectedLines called");
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
  supportsCodeAssistant(): Promise<boolean> {
    console.log("Checking whether code assistance is available");
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
};

const executableSelection = useExecutableSelectionStore();

const scriptingService =
  import.meta.env.VITE_SCRIPTING_API_MOCK === "true"
    ? getScriptingService(browserMockScriptingService)
    : getScriptingService();

export const pythonScriptingService = {
  initExecutableSelection: async (): Promise<void> => {
    const settings =
      (await scriptingService.getInitialSettings()) as PythonScriptingNodeSettings;
    setSelectedExecutable({ id: settings.executableSelection ?? "" });
    const executableInfo = (
      await pythonScriptingService.getExecutableOptionsList()
    ).find(({ id }) => id === executableSelection.id);
    if (
      typeof executableInfo === "undefined" ||
      executableInfo.type === "MISSING_VAR"
    ) {
      scriptingService.sendToConsole({
        text: `Flow variable "${executableSelection.id}" is missing, therefore no python executable could be started\n`,
      });
      setSelectedExecutable({ isMissing: true });
    }
  },
  saveSettings: (settings: NodeSettings) =>
    scriptingService.saveSettings(settings),
  getExecutableOptionsList: async (): Promise<ExecutableOption[]> => {
    return (await scriptingService.sendToService("getExecutableOptionsList", [
      executableSelection.id,
    ])) as ExecutableOption[];
  },
  startInteractivePythonSession: async (): Promise<StartSessionInfo> => {
    const startSessionInfo = (await scriptingService.sendToService(
      "startInteractive",
      [executableSelection.id],
    )) as StartSessionInfo;
    return startSessionInfo;
  },
  runScript: async (
    script: string,
    checkOutput: boolean = false,
  ): Promise<ExecutionInfo> => {
    const executionInfo = (await scriptingService.sendToService(
      "runInteractive",
      [script, checkOutput],
    )) as ExecutionInfo;
    return executionInfo;
  },
  killInteractivePythonSession: async () => {
    const killSessionInfo = (await scriptingService.sendToService(
      "killSession",
    )) as KillSessionInfo;
    return killSessionInfo;
  },
  getInputObjects: async (): Promise<InputPortInfo[]> => {
    return (await scriptingService.sendToService(
      "getInputObjects",
    )) as InputPortInfo[];
  },
  getAllLines: (): string | null => {
    return scriptingService.getScript();
  },
  getSelectedLines: (): string | null => {
    return scriptingService.getSelectedLines();
  },
  registerConsoleEventHandler: (handler: any) => {
    scriptingService.registerConsoleEventHandler(handler);
  },
  sendToConsole: (text: { text: string }) => {
    scriptingService.sendToConsole(text);
  },
  connectToLanguageServer: async () => {
    await scriptingService.connectToLanguageServer();

    // Configure the LSP server
    // TODO(AP-19349) get the current executable option id
    const config = JSON.parse(
      await scriptingService.sendToService("getLanguageServerConfig", [""]),
    );
    await scriptingService.configureLanguageServer(config);
  },
  closeDialog: (): void => {
    scriptingService.closeDialog();
  },
};
