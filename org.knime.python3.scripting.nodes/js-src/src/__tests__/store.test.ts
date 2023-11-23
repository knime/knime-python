import { getScriptingService } from "@knime/scripting-editor";
import { beforeEach, describe, expect, it, vi } from "vitest";

describe("store.ts", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  describe("sessionStatusStore", () => {
    const importSessionStatusStore = async () =>
      (await import("@/store")).useSessionStatusStore();

    beforeEach(() => {
      vi.resetModules();
    });

    it("should set isRunningSupported to true if inputs are available", async () => {
      const sessionStatus = await importSessionStatusStore();
      expect(sessionStatus.isRunningSupported).toBe(true);
    });

    it("should set isRunningSupported to false if no inputs are available", async () => {
      vi.mocked(getScriptingService().inputsAvailable).mockReturnValueOnce(
        Promise.resolve(false),
      );
      const sessionStatus = await importSessionStatusStore();
      expect(sessionStatus.isRunningSupported).toBe(false);
    });

    it("should log warning if no inputs are available", async () => {
      const scriptingService = getScriptingService();
      vi.mocked(scriptingService.inputsAvailable).mockReturnValueOnce(
        Promise.resolve(false),
      );
      await importSessionStatusStore();
      expect(scriptingService.sendToConsole).toHaveBeenCalledOnce();
      expect(scriptingService.sendToConsole).toHaveBeenCalledWith({
        warning:
          "Missing input data. Connect all input ports and execute preceding nodes to enable script execution.",
      });
    });
  });
});
