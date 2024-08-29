import "vitest-canvas-mock";
import {
  DEFAULT_INITIAL_DATA,
  DEFAULT_INITIAL_SETTINGS,
} from "@/__mocks__/mock-data";
import { vi } from "vitest";
import { Consola, LogLevel } from "consola";

export const consola = new Consola({
  level: LogLevel.Log,
});

vi.mock("@knime/ui-extension-service", async () => ({
  ...(await vi.importActual("@knime/ui-extension-service")),
  JsonDataService: {
    getInstance: vi.fn(() =>
      Promise.resolve({
        initialData: vi.fn(() =>
          Promise.resolve({
            settings: DEFAULT_INITIAL_SETTINGS,
            initialData: DEFAULT_INITIAL_DATA,
          }),
        ),
      }),
    ),
  },
  ReportingService: vi.fn(() => ({})),
}));

vi.mock("@/python-initial-data-service", () => ({
  getPythonInitialDataService: vi.fn(() => ({
    getInitialData: vi.fn(() => Promise.resolve(DEFAULT_INITIAL_DATA)),
  })),
}));

vi.mock("@/python-settings-service", () => ({
  getPythonSettingsService: vi.fn(() => ({
    getSettings: vi.fn(() => Promise.resolve(DEFAULT_INITIAL_SETTINGS)),
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
