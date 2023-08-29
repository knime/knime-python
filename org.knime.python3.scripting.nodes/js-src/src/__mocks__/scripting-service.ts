/* eslint-disable no-console */
import type {
  NodeSettings,
  ScriptingServiceType,
} from "@knime/scripting-editor";
import sleep from "webapps-common/util/sleep";

const SLEEP_TIME = 500;

const mockScriptingService: ScriptingServiceType = {
  async sendToService(methodName: string, options?: any[]) {
    console.log(`Called KNIME ${methodName} with ${JSON.stringify(options)}`);
    await sleep(SLEEP_TIME);
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
  getFlowVariableSettings() {
    return sleep(SLEEP_TIME);
  },
  async saveSettings(settings: NodeSettings) {
    console.log(`Saved settings ${JSON.stringify(settings)}`);
    await sleep(SLEEP_TIME);
  },
  registerEventHandler(type: string) {
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
    console.log(`initEditorService called with ${{ editor, editorModel }}`);
  },
  getScript() {
    console.log("getSelectedLines called");
    return "myScript";
  },
  getSelectedLines() {
    console.log("getSelectedLines called");
    return "mySelectedLines";
  },
};

export default mockScriptingService;
