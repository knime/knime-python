import { consoleHandler, getScriptingService } from "@knime/scripting-editor";

import { getPythonInitialDataService } from "./python-initial-data-service";
import { sendErrorToConsole } from "./send-error-to-console";
import {
  usePythonPreviewStatusStore,
  useSessionStatusStore,
  useWorkspaceStore,
} from "./store";
import type { ExecutionInfo } from "./types/common";

export const initPython = () => {
  const sessionStatus = useSessionStatusStore();

  // Handle execution finished events
  getScriptingService().registerEventHandler(
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

  // Check if inputs are available and set the isRunningSupported flag accordingly
  const initialData = getPythonInitialDataService().getInitialData();
  sessionStatus.isRunningSupported = initialData.inputConnectionInfo.every(
    (port) => port.isOptional || port.status === "OK",
  );

  if (!sessionStatus.isRunningSupported) {
    consoleHandler.writeln({
      warning:
        "Missing input data. Connect all input ports and execute preceding nodes to enable script execution.",
    });
  }
};
