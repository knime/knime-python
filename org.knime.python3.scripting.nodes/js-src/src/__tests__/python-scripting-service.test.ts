import {
  describe,
  it,
  vi,
  expect,
  beforeEach,
  afterEach,
  beforeAll,
} from "vitest";

import { getScriptingService } from "@knime/scripting-editor";
import { pythonScriptingService } from "@/python-scripting-service";
import { useSessionStatusStore, useWorkspaceStore } from "@/store";
import type { ExecutionInfo, KillSessionInfo } from "@/types/common";

describe("python-scripting-service", () => {
  let eventHandler: (info: ExecutionInfo) => void;

  beforeAll(() => {
    eventHandler = vi.mocked(getScriptingService().registerEventHandler).mock
      .calls[0][1];
  });

  it("should save settings", () => {
    const saveSettingsSpy = vi.spyOn(getScriptingService(), "saveSettings");

    pythonScriptingService.saveSettings({ script: "print('Hello')" });
    expect(saveSettingsSpy).toHaveBeenCalledOnce();
    expect(saveSettingsSpy).toHaveBeenCalledWith({
      script: "print('Hello')",
    });
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  describe("session handling", () => {
    beforeEach(() => {
      vi.mocked(getScriptingService().sendToService).mockImplementation(() =>
        Promise.resolve(),
      );
      useSessionStatusStore().status = "IDLE";
    });

    it("should run script", () => {
      pythonScriptingService.runScript();
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "runScript",
        ["myScript"],
      );
      expect(useSessionStatusStore().status).toBe("RUNNING");
    });

    it("should run selected lines", () => {
      pythonScriptingService.runSelectedLines();
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "runInExistingSession",
        ["mySelectedLines"],
      );
      expect(useSessionStatusStore().status).toBe("RUNNING");
    });

    it("should print variable", () => {
      pythonScriptingService.printVariable("myVariable");
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "runInExistingSession",
        ["print(myVariable)"],
      );
    });

    it("should update executable selection", () => {
      useSessionStatusStore().status = "RUNNING";
      useWorkspaceStore().workspace = [
        { name: "myVariable", type: "str", value: "Hello" },
      ];
      pythonScriptingService.updateExecutableSelection("myExecutable");
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "updateExecutableSelection",
        ["myExecutable"],
      );
      expect(useSessionStatusStore().status).toBe("IDLE");
      expect(useWorkspaceStore().workspace).toEqual([]);
    });

    describe("kill session", () => {
      beforeEach(() => {
        // Assume that a session is running for killing it
        useSessionStatusStore().status = "RUNNING";
        useWorkspaceStore().workspace = [
          { name: "myVariable", type: "str", value: "Hello" },
        ];
      });

      it("should kill session", async () => {
        vi.mocked(getScriptingService().sendToService).mockImplementation(
          (): Promise<KillSessionInfo> =>
            Promise.resolve({
              status: "SUCCESS",
              description: "Session killed",
            }),
        );
        await pythonScriptingService.killInteractivePythonSession();
        expect(getScriptingService().sendToService).toHaveBeenCalledWith(
          "killSession",
        );
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });

      it("should send error to console when killing session", async () => {
        vi.mocked(getScriptingService().sendToService).mockImplementation(
          (): Promise<KillSessionInfo> =>
            Promise.resolve({
              status: "ERROR",
              description: "My descriptive error message",
            }),
        );
        await pythonScriptingService.killInteractivePythonSession();
        expect(getScriptingService().sendToService).toHaveBeenCalledWith(
          "killSession",
        );
        expect(getScriptingService().sendToConsole).toHaveBeenCalledWith({
          text: "My descriptive error message\n",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });
    });

    describe("execution finished event handling", () => {
      beforeEach(() => {
        useSessionStatusStore().status = "RUNNING";
        useWorkspaceStore().workspace = [
          { name: "myVariable", type: "str", value: "Hello" },
        ];
      });

      it("should handle SUCCESS", () => {
        const expectedWorkspace = [
          { name: "myVariable", type: "str", value: "Hello" },
          { name: "myVariable2", type: "str", value: "World" },
        ];
        // const eventHandler = await getEventHandler();
        eventHandler({
          status: "SUCCESS",
          description: "Execution finished",
          data: expectedWorkspace,
        });
        expect(getScriptingService().sendToConsole).not.toHaveBeenCalled();
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual(expectedWorkspace);
      });

      it("should handle KNIME_ERROR", () => {
        eventHandler({
          status: "KNIME_ERROR",
          description: "KNIME error",
        });
        expect(getScriptingService().sendToConsole).toHaveBeenCalledWith({
          text: "KNIME error\n",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });

      it("should handle EXECUTION_ERROR", () => {
        eventHandler({
          status: "EXECUTION_ERROR",
          description: "Execution Error",
          traceback: ["line 1", "line 2"],
        });
        expect(getScriptingService().sendToConsole).toHaveBeenCalledWith({
          text: "Execution Error\nline 1\nline 2\n",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });

      it("should handle FATAL_ERROR", () => {
        eventHandler({
          status: "FATAL_ERROR",
          description: "My fatal Error",
        });
        expect(getScriptingService().sendToConsole).toHaveBeenCalledWith({
          text: "My fatal Error\n",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });

      it("should handle CANCELLED", () => {
        eventHandler({
          status: "CANCELLED",
          description: "Execution cancelled",
        });
        expect(getScriptingService().sendToConsole).not.toHaveBeenCalled();
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });
    });
  });
});
