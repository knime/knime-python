import { type InitMockData } from "@knime/scripting-editor";
import { createScriptingServiceMock } from "@knime/scripting-editor/scripting-service-browser-mock";
import { createSettingsServiceMock } from "@knime/scripting-editor/settings-service-browser-mock";

import { DEFAULT_INITIAL_DATA, DEFAULT_INITIAL_SETTINGS } from "./mock-data";

export default {
  scriptingService: createScriptingServiceMock({
    sendToServiceMockResponses: {
      getLanguageServerConfig: () => Promise.resolve(JSON.stringify({})),
    },
  }),
  settingsService: createSettingsServiceMock({
    settings: DEFAULT_INITIAL_SETTINGS,
  }),
  initialData: DEFAULT_INITIAL_DATA,
  displayMode: "large",
} satisfies InitMockData;
