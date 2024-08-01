import { DEFAULT_INITIAL_DATA } from "@/__mocks__/mock-data";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { consoleHandler, getInitialDataService } from "@knime/scripting-editor";

vi.mock("@knime/scripting-editor", async (importActual) => {
  const languageServerConnection = { changeConfiguration: vi.fn() };
  const mockScriptingService = {
    sendToService: vi.fn((args) => {
      // If this method is not mocked, the tests fail with a hard to debug
      // error otherwise, so we're really explicit here.
      throw new Error(
        `ScriptingService.sendToService should have been mocked for method ${args}`,
      );
    }),
    registerEventHandler: vi.fn(),
    connectToLanguageServer: vi.fn(() => languageServerConnection),
    registerSettingsGetterForApply: vi.fn(),
  };

  const scriptEditorModule = (await importActual()) as any;

  return {
    ...scriptEditorModule,
    getScriptingService: vi.fn(() => {
      return mockScriptingService;
    }),
    getInitialDataService: vi.fn(() => ({
      getInitialData: vi.fn(() => Promise.resolve(DEFAULT_INITIAL_DATA)),
      isInitialDataLoaded: vi.fn(() => true),
    })),
  };
});

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
      vi.mocked(getInitialDataService).mockReturnValue({
        getInitialData: vi.fn(() =>
          Promise.resolve({
            ...DEFAULT_INITIAL_DATA,
            inputsAvailable: false,
          }),
        ),
        isInitialDataLoaded: vi.fn(() => true),
      });
      const sessionStatus = await importSessionStatusStore();
      expect(sessionStatus.isRunningSupported).toBe(false);
    });

    it("should log warning if no inputs are available", async () => {
      vi.mocked(getInitialDataService).mockReturnValue({
        getInitialData: vi.fn(() =>
          Promise.resolve({
            ...DEFAULT_INITIAL_DATA,
            inputsAvailable: false,
          }),
        ),
        isInitialDataLoaded: vi.fn(() => true),
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
