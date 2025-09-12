import { beforeEach, describe, expect, it, vi } from "vitest";

import { consoleHandler, initMocked } from "@knime/scripting-editor";

import { DEFAULT_INITIAL_DATA } from "@/__mocks__/mock-data";
import { initPython } from "@/init";
import { useSessionStatusStore } from "@/store";

describe("init.ts", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
    vi.resetModules();
  });

  describe("initPython", () => {
    it("should set isRunningSupported to true if inputs are available", () => {
      const sessionStatus = useSessionStatusStore();
      initPython();

      expect(sessionStatus.isRunningSupported).toBe(true);
    });

    it("should set isRunningSupported to false if no inputs are available", () => {
      initMocked({
        initialData: {
          ...DEFAULT_INITIAL_DATA,
          inputConnectionInfo: [
            { status: "OK", isOptional: false },
            { status: "UNEXECUTED_CONNECTION", isOptional: false },
          ],
        },
      });
      const sessionStatus = useSessionStatusStore();
      initPython();

      expect(sessionStatus.isRunningSupported).toBe(false);
    });

    it("should log warning if no inputs are available", () => {
      initMocked({
        initialData: {
          ...DEFAULT_INITIAL_DATA,
          inputConnectionInfo: [
            { status: "OK", isOptional: false },
            { status: "UNEXECUTED_CONNECTION", isOptional: false },
          ],
        },
      });

      const consoleSpy = vi.spyOn(consoleHandler, "writeln");
      initPython();

      expect(consoleSpy).toHaveBeenCalledOnce();
      expect(consoleSpy).toHaveBeenCalledWith({
        warning:
          "Missing input data. Connect all input ports and execute preceding nodes to enable script execution.",
      });
    });
  });
});
