import { type Ref, reactive, ref } from "vue";

import type { SettingState } from "@knime/ui-extension-service";

import type { ExecutionResult, Workspace } from "./types/common";

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

type ControllingFlowVariableSetter = ReturnType<
  SettingState<string>["addControllingFlowVariable"]
>;
const executableSettingState = ref<ControllingFlowVariableSetter | null>(null);

export const useExecutableSelectionStore = (): ExecutableStore => {
  return selectedExecutable;
};

export const useExecutableSettingStore =
  (): Ref<ControllingFlowVariableSetter | null> => {
    return executableSettingState;
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
