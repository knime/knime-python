import { pythonScriptingService } from "@/python-scripting-service";
import {
  type ExecutionInfo,
  type ExecutionInfoWithTraceback,
  type ExecutionInfoWithWorkspace,
  type SessionInfo,
} from "@/types/common";
import { useWorkspaceStore } from "@/store";

const sessionInfoToString = (newSessionInfo: SessionInfo): string => {
  const { status, description } = newSessionInfo;
  return `${status}: ${description}\n`;
};

const executionInfoWithTracebackToString = (
  newExecutionInfo: ExecutionInfoWithTraceback,
): string => {
  return (
    `${sessionInfoToString(newExecutionInfo)}` +
    `${newExecutionInfo.traceback.join("\n")}\n`
  );
};

const sessionInfoToConsole = (
  newSessionInfo: SessionInfo,
  onlyShowErrors: boolean = true,
) => {
  const { status } = newSessionInfo;
  const output: { text: string | null } = { text: null };
  if (status.includes("ERROR") || status === "CANCELLED") {
    if (
      typeof (newSessionInfo as any).traceback === "undefined" ||
      (newSessionInfo as any).traceback === null
    ) {
      output.text = sessionInfoToString(newSessionInfo);
    } else {
      output.text = executionInfoWithTracebackToString(
        newSessionInfo as ExecutionInfoWithTraceback,
      );
    }
  } else if (!onlyShowErrors) {
    output.text = sessionInfoToString(newSessionInfo);
  }
  if (output.text !== null) {
    pythonScriptingService.sendToConsole(output as { text: string });
  }
};

/**
 * Handles output (text) generated by starting or killing a session or executing a script and sends it to the console.
 * @param newSessionInfo The status and description of the last action
 * @param onlyShowErrors If true, only errors are sent to console
 */
export const handleSessionInfo = (
  newSessionInfo: SessionInfo,
  onlyShowErrors: boolean = true,
) => {
  sessionInfoToConsole(newSessionInfo, onlyShowErrors);
};

/**
 * Updates the workspace that is returned from successful execution of a script
 * @param newExecutionInfo The status and description of the last action
 * @param onlyShowErrors If true, only errors are sent to console
 */
export const handleExecutionInfo = (newExecutionInfo: ExecutionInfo) => {
  const executionInfoWithWorkspace =
    newExecutionInfo as ExecutionInfoWithWorkspace;
  const workspace = executionInfoWithWorkspace.data;
  const store = useWorkspaceStore();
  store.workspace = workspace;
};