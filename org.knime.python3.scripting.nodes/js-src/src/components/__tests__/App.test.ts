import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { flushPromises, shallowMount } from "@vue/test-utils";
import App from "../App.vue";
import { ScriptingEditor, getScriptingService } from "@knime/scripting-editor";
import { executableOptionsMock } from "../../__mocks__/executable-options";
import { setSelectedExecutable, useExecutableSelectionStore } from "@/store";

describe("App.vue", () => {
  const executableOptions = executableOptionsMock;
  const executableSelectionStore = useExecutableSelectionStore();

  beforeEach(() => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(
      vi.fn((methodName: string): any => {
        if (methodName === "getExecutableOptionsList") {
          return executableOptions;
        } else {
          throw Error(`Method ${methodName} was not mocked but called in test`);
        }
      }),
    );
    setSelectedExecutable({ id: "", isMissing: false });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("renders the ScriptingEditor component with the correct language", () => {
    const wrapper = shallowMount(App);
    const scriptingComponent = wrapper.findComponent({
      name: "ScriptingEditor",
    });
    expect(scriptingComponent.exists()).toBeTruthy();
  });

  it("saves settings", () => {
    const wrapper = shallowMount(App);
    const scriptingEditor = wrapper.findComponent(ScriptingEditor);
    scriptingEditor.vm.$emit("save-settings", { script: "myScript" });
    expect(getScriptingService().saveSettings).toHaveBeenCalledWith({
      script: "myScript",
      executableSelection: "",
    });
  });

  it("initializes executable selection with initial settings", async () => {
    const settingsMock = {
      script: "",
      executableSelection: "conda.environment1",
    };
    getScriptingService().getInitialSettings = vi.fn((): any => settingsMock);
    shallowMount(App);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      [settingsMock.executableSelection],
    );
    expect(executableSelectionStore.id).toBe(settingsMock.executableSelection);
    expect(executableSelectionStore.isMissing).toBe(false);
    expect(getScriptingService().sendToConsole).not.toHaveBeenCalled();
  });

  it("sends error to console if executable is not in executable list", async () => {
    const settingsMock = {
      script: "",
      executableSelection: "unknown environment",
    };
    getScriptingService().getInitialSettings = vi.fn((): any => settingsMock);
    shallowMount(App);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      [settingsMock.executableSelection],
    );
    expect(getScriptingService().sendToConsole).toHaveBeenCalled();
  });

  it("sends error to console if executable is missing", async () => {
    const settingsMock = {
      script: "",
      executableSelection: "conda.environment1",
    };
    getScriptingService().getInitialSettings = vi.fn((): any => settingsMock);
    getScriptingService().sendToService = vi.fn((): any => [
      executableOptionsMock[0],
      {
        type: "MISSING_VAR",
        id: "conda.environment1",
      },
    ]);
    shallowMount(App);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      [settingsMock.executableSelection],
    );
    expect(getScriptingService().sendToConsole).toHaveBeenCalled();
  });
});
