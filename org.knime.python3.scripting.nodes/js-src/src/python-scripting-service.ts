import type {
  NodeSettings,
  ScriptingServiceType,
} from "@knime/scripting-editor";
import { getScriptingService } from "@knime/scripting-editor";
import {
  type ExecutableOption,
  type ExecutionInfo,
  type InputPortInfo,
  type KillSessionInfo,
  type StartSessionInfo,
} from "./types/common";

import { EditorService } from "@knime/scripting-editor/src/editor-service";
import sleep from "webapps-common/util/sleep";

/* eslint-disable no-console */

const SLEEP_TIME = 500;

const editorService = new EditorService();

const eventHandlers = new Map<string, (args: any) => void>();

const browserMockScriptingService: ScriptingServiceType = {
  async sendToService(methodName: string, options?: any[]) {
    console.log(`Called KNIME ${methodName} with ${JSON.stringify(options)}`);
    await sleep(SLEEP_TIME);
    if (methodName === "suggestCode") {
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
    }
    return {
      status: "SUCCESS",
      description: "mocked execution info",
      jsonFromExecution: null,
    };
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
};

const scriptingService =
  import.meta.env.VITE_SCRIPTING_API_MOCK === "true"
    ? getScriptingService(browserMockScriptingService)
    : getScriptingService();

export const pythonScriptingService = {
  saveSettings: (settings: NodeSettings) =>
    scriptingService.saveSettings(settings),
  getExecutableOptions: async (
    executableId: string = "",
  ): Promise<ExecutableOption[]> => {
    return (await scriptingService.sendToService("getExecutableOptions", [
      executableId,
    ])) as ExecutableOption[];
  },
  startInteractivePythonSession: async (
    executableId: string = "",
  ): Promise<StartSessionInfo> => {
    const startSessionInfo = (await scriptingService.sendToService(
      "startInteractive",
      [executableId],
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
};
