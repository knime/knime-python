import {
  getScriptingService,
  type NodeSettings,
} from "@knime/scripting-editor";

import type {
  ExecutableOption,
  ExecutionInfo,
  KillSessionInfo,
  PythonScriptingNodeSettings,
} from "./types/common";

import { registerInputCompletions } from "./input-completions";
import {
  setSelectedExecutable,
  useExecutableSelectionStore,
  useSessionStatusStore,
  useWorkspaceStore,
} from "./store";

const executableSelection = useExecutableSelectionStore();

const scriptingService =
  import.meta.env.VITE_SCRIPTING_API_MOCK === "true"
    ? getScriptingService(
        (await import("./browser-mock-scripting-service")).default,
      )
    : getScriptingService();

const sendErrorToConsole = ({
  description,
  traceback,
}: {
  description: string;
  traceback?: string[];
}) => {
  if (
    typeof traceback === "undefined" ||
    traceback === null ||
    traceback.length === 0
  ) {
    scriptingService.sendToConsole({
      error: `${description}\n`,
    });
  } else {
    scriptingService.sendToConsole({
      error: `${description}\n${traceback?.join("\n")}\n`,
    });
  }
};

// Handle execution finished events
scriptingService.registerEventHandler(
  "python-execution-finished",
  (info: ExecutionInfo) => {
    // Update the workspace
    useWorkspaceStore().workspace = info.data ?? [];

    // Send errors to the console
    if (
      info.status === "FATAL_ERROR" ||
      info.status === "EXECUTION_ERROR" ||
      info.status === "KNIME_ERROR"
    ) {
      sendErrorToConsole(info);
    }

    // Update the session status
    useSessionStatusStore().status = "IDLE";
  },
);

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
      sendErrorToConsole({
        description: `Flow variable "${executableSelection.id}" is missing, therefore no Python executable could be started\n`,
      });
      setSelectedExecutable({ isMissing: true });
    }
  },
  sendLastConsoleOutput: () => {
    scriptingService.sendToService("sendLastConsoleOutput");
  },
  saveSettings: (settings: NodeSettings) =>
    scriptingService.saveSettings(settings),
  getExecutableOptionsList: async (): Promise<ExecutableOption[]> => {
    return (await scriptingService.sendToService("getExecutableOptionsList", [
      executableSelection.id,
    ])) as ExecutableOption[];
  },
  runScript: () => {
    scriptingService.sendToService("runScript", [scriptingService.getScript()]);
    useSessionStatusStore().status = "RUNNING";
  },
  runSelectedLines: () => {
    scriptingService.sendToService("runInExistingSession", [
      scriptingService.getSelectedLines(),
    ]);
    useSessionStatusStore().status = "RUNNING";
  },
  printVariable: (variableName: string) => {
    scriptingService.sendToService("runInExistingSession", [
      `print(""">>> print(${variableName})\n""" + str(${variableName}))`,
    ]);
  },
  killInteractivePythonSession: async () => {
    const killSessionInfo: KillSessionInfo =
      await scriptingService.sendToService("killSession");

    // If there was an error, send it to the console
    if (killSessionInfo.status === "ERROR") {
      sendErrorToConsole(killSessionInfo);
    }

    // Cleanup the workspace and session status
    useWorkspaceStore().workspace = [];
    useSessionStatusStore().status = "IDLE";
  },
  updateExecutableSelection: (id: string) => {
    scriptingService.sendToService("updateExecutableSelection", [id]);

    // Cleanup the workspace and session status
    useWorkspaceStore().workspace = [];
    useSessionStatusStore().status = "IDLE";
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
  hasPreview: (): Promise<boolean> => {
    return scriptingService.sendToService("hasPreview");
  },
  registerInputCompletions: async () => {
    const inpObjects = await scriptingService.getInputObjects();
    const inpFlowVars = await scriptingService.getFlowVariableInputs();

    registerInputCompletions([
      ...inpObjects.flatMap(
        (inp, inpIdx) =>
          inp.subItems?.map((subItem) => ({
            label: subItem.name,
            detail: `input ${inpIdx} - column : ${subItem.type}`,
          })) ?? [],
      ),
      ...(inpFlowVars.subItems?.map((subItem) => ({
        label: subItem.name,
        detail: `flow variable : ${subItem.type}`,
      })) ?? []),
    ]);
  },
};
