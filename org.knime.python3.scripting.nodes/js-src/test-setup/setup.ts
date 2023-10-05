import { editorStoreMock } from "../src/__mocks__/editorStore";
import "vitest-canvas-mock";
import { vi } from "vitest";

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
    getScript() {
      return "myScript";
    },
    getSelectedLines() {
      return "mySelectedLines";
    },
    setScript: vi.fn(),
    registerEventHandler: vi.fn(),
    registerLanguageServerEventHandler: vi.fn(),
    registerConsoleEventHandler: vi.fn(),
    getLanguageServerConfig: vi.fn(),
    connectToLangaugeServer: vi.fn(),
    configureLanguageServer: vi.fn(),
    stopEventPoller: vi.fn(),
    sendToConsole: vi.fn(),
    initEditorService: vi.fn(),
    inputsAvailable: vi.fn(() => {
      return true;
    }),
    supportsCodeAssistant: vi.fn(() => {
      return true;
    }),
    closeDialog: vi.fn(),
  };

  const scriptEditorModule = await vi.importActual("@knime/scripting-editor");

  const useEditorStore = vi.fn(() => editorStoreMock);

  return {
    ...scriptEditorModule,
    getScriptingService: () => {
      return mockScriptingService;
    },
    useEditorStore,
  };
});
