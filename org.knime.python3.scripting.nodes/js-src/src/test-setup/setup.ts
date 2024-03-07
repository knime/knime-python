import "vitest-canvas-mock";
import { vi } from "vitest";
import { LogLevel } from "consola";

consola.level = LogLevel.Debug;

vi.mock("@knime/scripting-editor", async () => {
  const initialSettings = {
    script: "print('Hello World!')",
    executableSelection: "",
  };
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
    isCodeAssistantEnabled: vi.fn(() => Promise.resolve(true)),
    isCodeAssistantInstalled: vi.fn(() => Promise.resolve(true)),
    inputsAvailable: vi.fn(() => true),
    getInputObjects: vi.fn(() => []),
    getOutputObjects: vi.fn(() => []),
    getFlowVariableInputs: vi.fn(() => ({})),
    getInitialSettings: vi.fn(() => Promise.resolve(initialSettings)),
    registerSettingsGetterForApply: vi.fn(),
  };

  const scriptEditorModule = await vi.importActual("@knime/scripting-editor");

  // Replace the original implementations by the mocks
  // @ts-ignore
  Object.assign(scriptEditorModule.getScriptingService(), mockScriptingService);

  return {
    ...scriptEditorModule,
    getScriptingService: () => {
      return mockScriptingService;
    },
  };
});
