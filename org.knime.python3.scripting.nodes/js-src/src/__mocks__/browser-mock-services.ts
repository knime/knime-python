import {
  getInitialDataService,
  getScriptingService,
  getSettingsService,
} from "@knime/scripting-editor";
import { createInitialDataServiceMock } from "@knime/scripting-editor/initial-data-service-browser-mock";
import { createScriptingServiceMock } from "@knime/scripting-editor/scripting-service-browser-mock";
import { createSettingsServiceMock } from "@knime/scripting-editor/settings-service-browser-mock";

import { DEFAULT_INITIAL_DATA, DEFAULT_INITIAL_SETTINGS } from "./mock-data";

if (import.meta.env.MODE === "development.browser") {
  const scriptingService = createScriptingServiceMock({
    sendToServiceMockResponses: {
      getLanguageServerConfig: () => Promise.resolve(JSON.stringify({})),
    },
  });

  const initialDataService = createInitialDataServiceMock(DEFAULT_INITIAL_DATA);
  const settingsService = createSettingsServiceMock(DEFAULT_INITIAL_SETTINGS);

  Object.assign(getInitialDataService(), initialDataService);
  Object.assign(getScriptingService(), scriptingService);
  Object.assign(getSettingsService(), settingsService);
}
