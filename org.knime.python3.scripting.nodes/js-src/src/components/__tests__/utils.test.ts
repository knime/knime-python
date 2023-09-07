import { beforeEach, describe, vi, it, type SpyInstance, expect } from "vitest";
import {
  handleExecutionInfo,
  handleSessionInfo,
} from "../utils/handleSessionInfo";
import type {
  ExecutionInfoWithTraceback,
  ExecutionInfoWithWorkspace,
  SessionInfo,
} from "@/types/common";
import { getScriptingService } from "@knime/scripting-editor";
import { useWorkspaceStore } from "@/store";

describe("utils", () => {
  describe("handleSessionInfo", () => {
    let sendToConsoleSpy: SpyInstance;

    const successfulSessionInfo: SessionInfo = {
      status: "SUCCESS",
      description: "successful info",
    };

    const successfulExecutionInfo: ExecutionInfoWithWorkspace = {
      status: "SUCCESS",
      description: "successful info",
      data: [{ type: "int", name: "x", value: "1" }],
    };

    const executionInfoWithTraceback: ExecutionInfoWithTraceback = {
      status: "EXECUTION_ERROR",
      description: "an error occurred",
      traceback: ["this", "is", "a", "traceback"],
      data: [],
    };

    beforeEach(() => {
      sendToConsoleSpy = vi.spyOn(getScriptingService(), "sendToConsole");
    });

    it("does not print successful session info per default", () => {
      handleSessionInfo(successfulSessionInfo);
      expect(sendToConsoleSpy).not.toHaveBeenCalled();
    });

    it("does not print successful session info onlyShowErrors is true", () => {
      handleSessionInfo(successfulSessionInfo, true);
      expect(sendToConsoleSpy).not.toHaveBeenCalled();
    });

    it("prints successful session info if onlyShowErrors is false", () => {
      handleSessionInfo(successfulSessionInfo, false);
      expect(sendToConsoleSpy).toHaveBeenCalledWith({
        text: `${successfulSessionInfo.status}: ${successfulSessionInfo.description}\n`,
      });
    });

    it("prints error message", () => {
      handleSessionInfo(executionInfoWithTraceback);
      expect(sendToConsoleSpy).toHaveBeenCalledWith({
        text:
          `${executionInfoWithTraceback.status}: ${executionInfoWithTraceback.description}\n` +
          `${executionInfoWithTraceback.traceback.join("\n")}\n`,
      });
    });

    it("stores workspace", () => {
      const store = useWorkspaceStore();
      expect(store.workspace).toBeUndefined();
      handleExecutionInfo(successfulExecutionInfo);
      expect(store.workspace).toStrictEqual(successfulExecutionInfo.data);
    });
  });
});
