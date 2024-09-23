import { consoleHandler } from "@knime/scripting-editor";
import type { ExecutionResult, Workspace } from "./types/common";
import { reactive } from "vue";
import { getPythonInitialDataService } from "./python-initial-data-service";

export type WorkspaceStore = {
  workspace?: Workspace;
};

const workspace: WorkspaceStore = reactive<WorkspaceStore>({});

export const useWorkspaceStore = (): WorkspaceStore => {
  return workspace;
};

export type ExecutableStore = {
  id: string;
  /** the value selected on the preference page, applied to "id" when the preference page is closed */
  livePrefValue: string | null;
  isMissing: boolean;
};

const selectedExecutable: ExecutableStore = reactive<ExecutableStore>({
  id: "",
  livePrefValue: null,
  isMissing: false,
});

export const useExecutableSelectionStore = (): ExecutableStore => {
  return selectedExecutable;
};

export const setSelectedExecutable = (args: Partial<ExecutableStore>): void => {
  if (typeof args.id !== "undefined") {
    selectedExecutable.id = args.id;
  }

  if (typeof args.isMissing !== "undefined") {
    selectedExecutable.isMissing = args.isMissing;
  }
};

export type SessionStatus = "IDLE" | "RUNNING_ALL" | "RUNNING_SELECTED";
type LastActionStatus = ExecutionResult | "RESET" | "RESET_FAILED";
export type SessionStatusStore = {
  status: SessionStatus;
  lastActionResult?: LastActionStatus;
  isRunningSupported: boolean;
};

const sessionStatus: SessionStatusStore = reactive<SessionStatusStore>({
  status: "IDLE",
  isRunningSupported: false,
});

export const useSessionStatusStore = (): SessionStatusStore => {
  return sessionStatus;
};

// Check if inputs are available and set the isRunningSupported flag accordingly
getPythonInitialDataService()
  .getInitialData()
  .then((initialData) => {
    sessionStatus.isRunningSupported = initialData.inputConnectionInfo.every(
      (port) => port.isOptional || port.status === "OK",
    );

    if (!sessionStatus.isRunningSupported) {
      consoleHandler.writeln({
        warning:
          "Missing input data. Connect all input ports and execute preceding nodes to enable script execution.",
      });
    }
  });

export type PythonViewStatus = {
  hasValidView: boolean;
  isExecutedOnce: boolean;
  updateViewCallback?: () => void;
  clearView: () => void;
};

const pythonPreviewStatus: PythonViewStatus = reactive<PythonViewStatus>({
  hasValidView: false,
  isExecutedOnce: false,
  clearView() {
    this.hasValidView = false;
    this.isExecutedOnce = false;
  },
});

export const usePythonPreviewStatusStore = (): PythonViewStatus =>
  pythonPreviewStatus;
