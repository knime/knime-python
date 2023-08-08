/* eslint-disable no-console */
import type {
  NodeSettings,
  ScriptingServiceType,
} from "@knime/scripting-editor";
import sleep from "webapps-common/util/sleep";

const SLEEP_TIME = 500;

const mockScriptingService: ScriptingServiceType = {
  sendToService(methodName: string, options?: any[]) {
    console.log(`Called KNIME ${methodName} with ${JSON.stringify(options)}`);
    return sleep(SLEEP_TIME);
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
};

export default mockScriptingService;
