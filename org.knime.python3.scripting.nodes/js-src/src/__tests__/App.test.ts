import { describe, it, expect, vi } from "vitest";
import { shallowMount, mount } from "@vue/test-utils";
import Button from "webapps-common/ui/components/Button.vue";
import { CodeEditor } from "@knime/scripting-editor";
import App from "../App.vue";
import { pythonScriptingService } from "@/python-scripting-service";

describe("App.vue", () => {
  it("renders the ScriptingEditor component with the correct language", () => {
    const wrapper = shallowMount(App);
    const scriptingComponent = wrapper.findComponent({
      name: "ScriptingEditor",
    });
    expect(scriptingComponent).toBeDefined();
  });

  it("saves the settings on button click", () => {
    const saveSettingsSpy = vi.spyOn(pythonScriptingService, "saveSettings");
    const wrapper = mount(App);

    // Fake the editor model
    const codeEditor = wrapper.findComponent(CodeEditor);
    codeEditor.vm.$emit("monaco-created", {
      editor: null,
      editorModel: { getValue: () => "print('Hello')" },
    });

    // Trigger a save settings button click
    wrapper
      .findAllComponents(Button)
      .filter((b) => b.text() === "Save Settings")[0]
      .find("button")
      .trigger("click");

    expect(saveSettingsSpy).toHaveBeenCalledOnce();
    expect(saveSettingsSpy).toHaveBeenCalledWith({ script: "print('Hello')" });
  });
});
