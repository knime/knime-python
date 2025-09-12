import { watch } from "vue";

import {
  type GenericInitialData,
  type InputOutputModel,
  consoleHandler,
  editor,
  getInitialData,
  getScriptingService,
  getSettingsService,
} from "@knime/scripting-editor";

import { registerInputCompletions } from "./input-completions";
import { sendErrorToConsole } from "./send-error-to-console";
import {
  setSelectedExecutable,
  useExecutableSelectionStore,
  useSessionStatusStore,
  useWorkspaceStore,
} from "./store";
import type { ExecutableOption, KillSessionInfo } from "./types/common";

export type PythonInitialData = GenericInitialData & {
  hasPreview: boolean;
  outputObjects: InputOutputModel[];
  executableOptionsList: ExecutableOption[];
};

const executableSelection = useExecutableSelectionStore();
const sessionStatus = useSessionStatusStore();
const mainEditorState = editor.useMainCodeEditorStore();

export const getPythonInitialData = () => getInitialData() as PythonInitialData;

export const pythonScriptingService = {
  initExecutableSelection: (): void => {
    const settings = getSettingsService().getSettings();

    setSelectedExecutable({ id: settings.executableSelection ?? "" });
    pythonScriptingService.updateExecutableSelection(executableSelection.id);
    const executableInfo = getPythonInitialData().executableOptionsList.find(
      ({ id }) => id === executableSelection.id,
    );
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
    getScriptingService().sendToService("sendLastConsoleOutput");
  },
  runScript: () => {
    getScriptingService().sendToService("runScript", [
      mainEditorState.value?.text.value,
    ]);
    sessionStatus.status = "RUNNING_ALL";
  },
  runSelectedLines: () => {
    getScriptingService().sendToService("runInExistingSession", [
      mainEditorState.value?.selectedLines.value,
    ]);
    sessionStatus.status = "RUNNING_SELECTED";
  },
  printVariable: (variableName: string) => {
    getScriptingService().sendToService("runInExistingSession", [
      `print(""">>> print(${variableName})\n""" + str(${variableName}))`,
    ]);
  },
  killInteractivePythonSession: async () => {
    const killSessionInfo: KillSessionInfo =
      await getScriptingService().sendToService("killSession");

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
    getScriptingService().sendToService("updateExecutableSelection", [id]);

    // Cleanup the workspace and session status
    useWorkspaceStore().workspace = [];
    sessionStatus.status = "IDLE";
  },
  sendToConsole: (text: { text: string }) => {
    consoleHandler.writeln(text);
  },
  connectToLanguageServer: async () => {
    try {
      const connection = await getScriptingService().connectToLanguageServer();
      const configureLanguageServer = async (id: string) => {
        const config = JSON.parse(
          await getScriptingService().sendToService("getLanguageServerConfig", [
            id,
          ]),
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
  registerInputCompletions: () => {
    const initialData = getPythonInitialData();
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
