import { describe, it, vi, expect } from "vitest";

import { pythonScriptingService } from "@/python-scripting-service";
import mockScriptingService from "@/__mocks__/scripting-service";

describe("python-scripting-service", () => {
  it("should save settings", () => {
    const saveSettingsSpy = vi.spyOn(mockScriptingService, "saveSettings");

    pythonScriptingService.saveSettings({ script: "print('Hello')" });
    expect(saveSettingsSpy).toHaveBeenCalledOnce();
    expect(saveSettingsSpy).toHaveBeenCalledWith({
      script: "print('Hello')",
    });
  });
});
