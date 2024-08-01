import { createScriptingServiceMock } from "@knime/scripting-editor/scripting-service-browser-mock";
import {
  getInitialDataService,
  getScriptingService,
} from "@knime/scripting-editor";
import { DEFAULT_INITIAL_DATA } from "./mock-data";
import { createInitialDataServiceMock } from "@knime/scripting-editor/initial-data-service-browser-mock";

if (import.meta.env.MODE === "development.browser") {
  const scriptingService = createScriptingServiceMock({
    sendToServiceMockResponses: {
      getLanguageServerConfig: () => Promise.resolve(JSON.stringify({})),
      hasPreview: () => Promise.resolve(true),
      getExecutableOptionsList: () => Promise.resolve([]),
    },
    initialSettings: DEFAULT_INITIAL_DATA,
  });

  const initialDataService = createInitialDataServiceMock(DEFAULT_INITIAL_DATA);

  Object.assign(getInitialDataService(), initialDataService);
  Object.assign(getScriptingService(), scriptingService);
}
