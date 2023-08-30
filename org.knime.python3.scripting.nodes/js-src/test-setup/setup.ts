import "vitest-canvas-mock";
import { vi } from "vitest";

const mockScriptingService = {
  sendToService: vi.fn(() => {
    // If this method is not mocked, the tests fail with a hard to debug
    // error otherwise, so we're really explicit here.
    throw new Error("should have been mocked");
  }),
  getInitialSettings() {
    return { script: "print('Hello World!')" };
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
  stopEventPoller: vi.fn(),
  sendToConsole: vi.fn(),
  initEditorService: vi.fn(),
};

vi.mock("@knime/scripting-editor", async () => {
  const scriptEditorModule = await vi.importActual("@knime/scripting-editor");

  return {
    ...scriptEditorModule,
    getScriptingService: () => {
      return mockScriptingService;
    },
  };
});
