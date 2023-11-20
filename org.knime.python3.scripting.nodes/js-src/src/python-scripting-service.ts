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
  usePythonPreviewStatusStore,
  useSessionStatusStore,
  useWorkspaceStore,
} from "./store";
import { watch } from "vue";

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

    // update view status
    const pythonPreviewStatus = usePythonPreviewStatusStore();
    pythonPreviewStatus.hasValidView = Boolean(info.hasValidView);
    pythonPreviewStatus.isExecutedOnce = true;
    if (
      pythonPreviewStatus.hasValidView &&
      pythonPreviewStatus.updateViewCallback
    ) {
      pythonPreviewStatus.updateViewCallback();
    }
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
    const configureLanguageServer = async (id: string) => {
      const config = JSON.parse(
        await scriptingService.sendToService("getLanguageServerConfig", [id]),
      );
      await scriptingService.configureLanguageServer(config);
    };
    await configureLanguageServer(executableSelection.id);

    // Watch the executable selection and re-configure on every change
    watch(
      () => executableSelection.id,
      (newId) => {
        configureLanguageServer(newId);
      },
    );
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
