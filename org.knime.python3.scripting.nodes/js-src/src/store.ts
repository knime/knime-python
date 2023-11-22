import type { ExecutionResult, Workspace } from "./types/common";
import { reactive } from "vue";

export type WorkspaceStore = {
  workspace?: Workspace;
};

const workspace: WorkspaceStore = reactive<WorkspaceStore>({});

export const useWorkspaceStore = (): WorkspaceStore => {
  return workspace;
};

export type ExecutableStore = {
  id: string;
  isMissing: boolean;
};

const selectedExecutable: ExecutableStore = reactive<ExecutableStore>({
  id: "",
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
};

const sessionStatus: SessionStatusStore = reactive<SessionStatusStore>({
  status: "IDLE",
});

export const useSessionStatusStore = (): SessionStatusStore => {
  return sessionStatus;
};

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
