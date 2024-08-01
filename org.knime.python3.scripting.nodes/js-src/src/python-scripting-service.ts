import { watch } from "vue";

import {
  editor,
  getScriptingService,
  consoleHandler,
} from "@knime/scripting-editor";
import { registerInputCompletions } from "./input-completions";
import {
  setSelectedExecutable,
  useExecutableSelectionStore,
  usePythonPreviewStatusStore,
  useSessionStatusStore,
  useWorkspaceStore,
} from "./store";
import type { ExecutionInfo, KillSessionInfo } from "./types/common";
import { getPythonInitialDataService } from "./python-initial-data-service";
import { getPythonSettingsService } from "./python-settings-service";

const scriptingService = getScriptingService();
const executableSelection = useExecutableSelectionStore();
const sessionStatus = useSessionStatusStore();

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
    consoleHandler.writeln({
      error: description,
    });
  } else {
    consoleHandler.writeln({
      error: `${description}\n${traceback?.join("\n")}`,
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
    sessionStatus.status = "IDLE";

    // Update the last execution result
    sessionStatus.lastActionResult = info.status;

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

const mainEditorState = editor.useMainCodeEditorStore();

export const pythonScriptingService = {
  initExecutableSelection: async (): Promise<void> => {
    const settings = await getPythonSettingsService().getSettings();

    setSelectedExecutable({ id: settings.executableSelection ?? "" });
    pythonScriptingService.updateExecutableSelection(executableSelection.id);
    const executableInfo = (
      await getPythonInitialDataService().getInitialData()
    ).executableOptionsList.find(({ id }) => id === executableSelection.id);
    if (
      typeof executableInfo === "undefined" ||
      executableInfo.type === "MISSING_VAR"
    ) {
      sendErrorToConsole({
        description: `Flow variable "${executableSelection.id}" is missing, therefore no Python executable could be started`,
      });
      setSelectedExecutable({ isMissing: true });
    }
  },
  sendLastConsoleOutput: () => {
    scriptingService.sendToService("sendLastConsoleOutput");
  },
  runScript: () => {
    scriptingService.sendToService("runScript", [
      mainEditorState.value?.text.value,
    ]);
    sessionStatus.status = "RUNNING_ALL";
  },
  runSelectedLines: () => {
    scriptingService.sendToService("runInExistingSession", [
      mainEditorState.value?.selectedLines.value,
    ]);
    sessionStatus.status = "RUNNING_SELECTED";
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
    sessionStatus.status = "IDLE";
    return killSessionInfo.status === "SUCCESS";
  },
  updateExecutableSelection: (id: string) => {
    scriptingService.sendToService("updateExecutableSelection", [id]);

    // Cleanup the workspace and session status
    useWorkspaceStore().workspace = [];
    sessionStatus.status = "IDLE";
  },
  sendToConsole: (text: { text: string }) => {
    consoleHandler.writeln(text);
  },
  connectToLanguageServer: async () => {
    try {
      const connection = await scriptingService.connectToLanguageServer();
      const configureLanguageServer = async (id: string) => {
        const config = JSON.parse(
          await scriptingService.sendToService("getLanguageServerConfig", [id]),
        );
        await connection.changeConfiguration(config);
      };
      await configureLanguageServer(executableSelection.id);

      // Watch the executable selection and re-configure on every change
      watch(
        () => executableSelection.id,
        (newId) => {
          configureLanguageServer(newId);
        },
      );
    } catch (e: any) {
      consoleHandler.writeln({ warning: e.message });
    }
  },
  registerInputCompletions: async () => {
    const initialData = await getPythonInitialDataService().getInitialData();
    const inpObjects = initialData.inputObjects;
    const inpFlowVars = initialData.flowVariables;

    registerInputCompletions([
      ...inpObjects.flatMap(
        (inp, inpIdx) =>
          inp.subItems
            ?.filter((subItem) => subItem.supported)
            .map((subItem) => ({
              label: subItem.name.replace(/\\/g, "\\\\").replace(/"/g, '\\"'),
              detail: `input ${inpIdx} - column : ${subItem.type}`,
            })) ?? [],
      ),
      ...(inpFlowVars.subItems
        ?.filter((subItem) => subItem.supported)
        .map((subItem) => ({
          label: subItem.name.replace(/\\/g, "\\\\").replace(/"/g, '\\"'),
          detail: `flow variable : ${subItem.type}`,
        })) ?? []),
    ]);
  },
};
