import { describe, it, vi, expect } from "vitest";

import { getScriptingService } from "@knime/scripting-editor";
import { pythonScriptingService } from "@/python-scripting-service";

describe("python-scripting-service", () => {
  it("should save settings", () => {
    const saveSettingsSpy = vi.spyOn(getScriptingService(), "saveSettings");

    pythonScriptingService.saveSettings({ script: "print('Hello')" });
    expect(saveSettingsSpy).toHaveBeenCalledOnce();
    expect(saveSettingsSpy).toHaveBeenCalledWith({
      script: "print('Hello')",
    });
  });
});
