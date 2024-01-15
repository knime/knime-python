import "vitest-canvas-mock";
import { vi } from "vitest";
import { LogLevel } from "consola";

consola.level = LogLevel.Debug;

vi.mock("@knime/scripting-editor", async () => {
  const mockScriptingService = {
    sendToService: vi.fn((args) => {
      // If this method is not mocked, the tests fail with a hard to debug
      // error otherwise, so we're really explicit here.
      throw new Error(
        `ScriptingService.sendToService should have been mocked for method ${args}`,
      );
    }),
    getInitialSettings() {
      return { script: "print('Hello World!')", executableSelection: "" };
    },
    saveSettings: vi.fn(() => {}),
    registerEventHandler: vi.fn(),
    getLanguageServerConfig: vi.fn(),
    connectToLanguageServer: vi.fn(),
    configureLanguageServer: vi.fn(),
    stopEventPoller: vi.fn(),
    sendToConsole: vi.fn(),
    inputsAvailable: vi.fn(() => {
      return true;
    }),
    supportsCodeAssistant: vi.fn(() => {
      return true;
    }),
    closeDialog: vi.fn(),
    getInputObjects: vi.fn(() => []),
    getFlowVariableInputs: vi.fn(() => ({})),
  };

  const scriptEditorModule = await vi.importActual("@knime/scripting-editor");

  return {
    ...scriptEditorModule,
    getScriptingService: () => {
      return mockScriptingService;
    },
  };
});
