import "vitest-canvas-mock";
import {
  DEFAULT_INITIAL_DATA,
  DEFAULT_INITIAL_SETTINGS,
} from "@/__mocks__/mock-data";
import { vi } from "vitest";
import { Consola, LogLevel } from "consola";
import { ref } from "vue";

export const consola = new Consola({
  level: LogLevel.Log,
});

vi.mock("@/python-initial-data-service", () => ({
  getPythonInitialDataService: vi.fn(() => ({
    getInitialData: vi.fn(() => Promise.resolve(DEFAULT_INITIAL_DATA)),
    isInitialDataLoaded: vi.fn(() => true),
  })),
}));

vi.mock("@/python-settings-service", () => ({
  getPythonSettingsService: vi.fn(() => ({
    getSettings: vi.fn(() => Promise.resolve(DEFAULT_INITIAL_SETTINGS)),
    areSettingsLoaded: vi.fn(() => ref(true)),
    registerSettingsGetterForApply: vi.fn(() => Promise.resolve()),
  })),
}));

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
  };

  const scriptEditorModule = (await importActual()) as any;

  return {
    ...scriptEditorModule,
    getScriptingService: vi.fn(() => {
      return mockScriptingService;
    }),
  };
});
