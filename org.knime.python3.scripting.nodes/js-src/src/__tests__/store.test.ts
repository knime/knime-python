import { DEFAULT_INITIAL_DATA } from "@/__mocks__/mock-data";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { consoleHandler } from "@knime/scripting-editor";
import { getPythonInitialDataService } from "@/python-initial-data-service";

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
      vi.mocked(getPythonInitialDataService).mockReturnValue({
        getInitialData: () =>
          Promise.resolve({
            ...DEFAULT_INITIAL_DATA,
            inputsAvailable: false,
          }),
      });
      const sessionStatus = await importSessionStatusStore();
      expect(sessionStatus.isRunningSupported).toBe(false);
    });

    it("should log warning if no inputs are available", async () => {
      vi.mocked(getPythonInitialDataService).mockReturnValue({
        getInitialData: () =>
          Promise.resolve({
            ...DEFAULT_INITIAL_DATA,
            inputsAvailable: false,
          }),
      });

      const consoleSpy = vi.spyOn(consoleHandler, "writeln");

      await importSessionStatusStore();

      expect(consoleSpy).toHaveBeenCalledOnce();
      expect(consoleSpy).toHaveBeenCalledWith({
        warning:
          "Missing input data. Connect all input ports and execute preceding nodes to enable script execution.",
      });
    });
  });
});
