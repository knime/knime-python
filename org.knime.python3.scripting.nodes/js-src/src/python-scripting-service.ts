import type { NodeSettings } from "@knime/scripting-editor";
import { getScriptingService } from "@knime/scripting-editor";

const scriptingService =
  import.meta.env.VITE_SCRIPTING_API_MOCK === "true"
    ? getScriptingService(
        (await import("@/__mocks__/scripting-service")).default,
      )
    : getScriptingService();

export const pythonScriptingService = {
  saveSettings: (settings: NodeSettings) =>
    scriptingService.saveSettings(settings),
};
