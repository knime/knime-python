import {
  describe,
  it,
  vi,
  expect,
  beforeEach,
  afterEach,
  beforeAll,
} from "vitest";

import {
  getScriptingService,
  editor,
  consoleHandler,
} from "@knime/scripting-editor";
import { pythonScriptingService } from "@/python-scripting-service";
import { useSessionStatusStore, useWorkspaceStore } from "@/store";
import type { ExecutionInfo, KillSessionInfo } from "@/types/common";
import { ref } from "vue";

describe("python-scripting-service", () => {
  let eventHandler: (info: ExecutionInfo) => void;
  const getConsoleSpy = () => vi.spyOn(consoleHandler, "writeln");

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
      editor.useMainCodeEditorStore().value = { text: ref("myScript") } as any;

      pythonScriptingService.runScript();
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "runScript",
        ["myScript"],
      );
      expect(useSessionStatusStore().status).toBe("RUNNING_ALL");
    });

    it("should run selected lines", () => {
      editor.useMainCodeEditorStore().value = {
        selectedLines: ref("mySelectedLines"),
      } as any;

      pythonScriptingService.runSelectedLines();
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "runInExistingSession",
        ["mySelectedLines"],
      );
      expect(useSessionStatusStore().status).toBe("RUNNING_SELECTED");
    });

    it("should print variable", () => {
      pythonScriptingService.printVariable("myVariable");
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "runInExistingSession",
        ['print(""">>> print(myVariable)\n""" + str(myVariable))'],
      );
    });

    it("should update executable selection", () => {
      useSessionStatusStore().status = "RUNNING_ALL";
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
        useSessionStatusStore().status = "RUNNING_ALL";
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
        const consoleSpy = getConsoleSpy();
        await pythonScriptingService.killInteractivePythonSession();
        expect(getScriptingService().sendToService).toHaveBeenCalledWith(
          "killSession",
        );
        expect(consoleSpy).toHaveBeenCalledWith({
          error: "My descriptive error message",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });
    });

    describe("execution finished event handling", () => {
      beforeEach(() => {
        useSessionStatusStore().status = "RUNNING_ALL";
        useWorkspaceStore().workspace = [
          { name: "myVariable", type: "str", value: "Hello" },
        ];
      });

      it("should handle SUCCESS", () => {
        const expectedWorkspace = [
          { name: "myVariable", type: "str", value: "Hello" },
          { name: "myVariable2", type: "str", value: "World" },
        ];
        const consoleSpy = getConsoleSpy();
        eventHandler({
          status: "SUCCESS",
          description: "Execution finished",
          data: expectedWorkspace,
        });
        expect(consoleSpy).not.toHaveBeenCalled();
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useSessionStatusStore().lastActionResult).toBe("SUCCESS");
        expect(useWorkspaceStore().workspace).toEqual(expectedWorkspace);
      });

      it("should handle KNIME_ERROR", () => {
        const consoleSpy = getConsoleSpy();
        eventHandler({
          status: "KNIME_ERROR",
          description: "KNIME error",
        });
        expect(consoleSpy).toHaveBeenCalledWith({
          error: "KNIME error",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useSessionStatusStore().lastActionResult).toBe("KNIME_ERROR");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });

      it("should handle EXECUTION_ERROR", () => {
        const consoleSpy = getConsoleSpy();
        eventHandler({
          status: "EXECUTION_ERROR",
          description: "Execution Error",
          traceback: ["line 1", "line 2"],
        });
        expect(consoleSpy).toHaveBeenCalledWith({
          error: "Execution Error\nline 1\nline 2",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useSessionStatusStore().lastActionResult).toBe(
          "EXECUTION_ERROR",
        );
        expect(useWorkspaceStore().workspace).toEqual([]);
      });

      it("should handle FATAL_ERROR", () => {
        const consoleSpy = getConsoleSpy();
        eventHandler({
          status: "FATAL_ERROR",
          description: "My fatal Error",
        });
        expect(consoleSpy).toHaveBeenCalledWith({
          error: "My fatal Error",
        });
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useSessionStatusStore().lastActionResult).toBe("FATAL_ERROR");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });

      it("should handle CANCELLED", () => {
        const consoleSpy = getConsoleSpy();
        eventHandler({
          status: "CANCELLED",
          description: "Execution cancelled",
        });
        expect(consoleSpy).not.toHaveBeenCalled();
        expect(useSessionStatusStore().status).toBe("IDLE");
        expect(useSessionStatusStore().lastActionResult).toBe("CANCELLED");
        expect(useWorkspaceStore().workspace).toEqual([]);
      });
    });
  });
});
