import { getSettingsService } from "@knime/scripting-editor";

import type { PythonScriptingNodeSettings } from "./types/common";

export const getPythonSettingsService = () => ({
  ...getSettingsService(),
  getSettings: async () =>
    (await getSettingsService().getSettings()) as PythonScriptingNodeSettings,
});

export type PythonSettingsService = ReturnType<typeof getPythonSettingsService>;
