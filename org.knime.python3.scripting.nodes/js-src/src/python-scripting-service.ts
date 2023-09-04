import type { NodeSettings } from "@knime/scripting-editor";
import { getScriptingService } from "@knime/scripting-editor";
import {
  type ExecutableOption,
  type ExecutionInfo,
  type InputPortInfo,
  type KillSessionInfo,
  type StartSessionInfo,
} from "./types/common";

const scriptingService =
  import.meta.env.VITE_SCRIPTING_API_MOCK === "true"
    ? getScriptingService(
        (await import("@/__mocks__/scripting-service")).default,
      )
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
  connectToLanguageServer: async (editorModel: any) => {
    await scriptingService.connectToLanguageServer(editorModel);

    // Configure the LSP server
    // TODO(AP-19349) get the current executable option id
    const config = JSON.parse(
      await scriptingService.sendToService("getLanguageServerConfig", [""]),
    );
    await scriptingService.configureLanguageServer(config);
  },
};
