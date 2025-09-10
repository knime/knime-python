import "vitest-canvas-mock";
import { vi } from "vitest";
import { Consola, LogLevels } from "consola";

import { initMocked } from "@knime/scripting-editor";

import {
  DEFAULT_INITIAL_DATA,
  DEFAULT_INITIAL_SETTINGS,
} from "@/__mocks__/mock-data";

export const consola = new Consola({
  level: LogLevels.log,
});
// @ts-expect-error TODO how to tell TS that consola is a global?
window.consola = consola;

// Prevent the usage of @knime/ui-extension-service in the tests. Using it could lead to
// hard-to-debug failures due to timeouts.
vi.mock("@knime/ui-extension-service", () => ({}));

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

// Initialize @knime/scripting-editor with mock data
initMocked({
  scriptingService: {
    sendToService: vi.fn(),
    getOutputPreviewTableInitialData: vi.fn(() => Promise.resolve(undefined)),
    registerEventHandler: vi.fn(),
    connectToLanguageServer: vi.fn(),
    isKaiEnabled: vi.fn(),
    isLoggedIntoHub: vi.fn(),
    getAiDisclaimer: vi.fn(),
    getAiUsage: vi.fn(),
    isCallKnimeUiApiAvailable: vi.fn(() => Promise.resolve(false)),
  },
  settingsService: {
    getSettings: vi.fn(() => Promise.resolve(DEFAULT_INITIAL_SETTINGS)),
    registerSettingsGetterForApply: vi.fn(),
    registerSettings: vi.fn(() => vi.fn()),
  },
  initialData: DEFAULT_INITIAL_DATA,
  displayMode: "small",
});
