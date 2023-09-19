import { setSelectedExecutable, useExecutableSelectionStore } from "@/store";
import { ScriptingEditor, getScriptingService } from "@knime/scripting-editor";
import { VueWrapper, flushPromises, mount } from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import { executableOptionsMock } from "../../__mocks__/executable-options";
import App from "../App.vue";

describe("App.vue", () => {
  const doMount = async ({
    viewAvailable = false,
    executableOptions = executableOptionsMock,
  }: {
    viewAvailable?: boolean;
    executableOptions?: any[];
  } = {}) => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(
      (methodName: string): any => {
        if (methodName === "hasPreview") {
          return Promise.resolve(viewAvailable);
        } else if (methodName === "getExecutableOptionsList") {
          return executableOptions;
        } else {
          throw Error(`Method ${methodName} was not mocked but called in test`);
        }
      },
    );
    setSelectedExecutable({ id: "", isMissing: false });
    const wrapper = mount(App, {
      global: {
        stubs: {
          TabBar: true,
          OutputConsole: true,
          PythonEditorControls: true,
        },
      },
    });
    await flushPromises(); // to wait for PythonEditorControls.onMounted to finish
    return { wrapper };
  };

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders the ScriptingEditor component with the correct language", async () => {
    const { wrapper } = await doMount();
    const scriptingComponent = wrapper.findComponent({
      name: "ScriptingEditor",
    });
    expect(scriptingComponent.exists()).toBeTruthy();
    expect(scriptingComponent.props("language")).toBe("python");
    expect(scriptingComponent.props("fileName")).toBe("main.py");
  });

  describe("right panel", () => {
    const findComponents = (wrapper: VueWrapper) => {
      const tabbar = wrapper.findComponent({ name: "TabBar" });
      const workspace = wrapper.findComponent({ name: "PythonWorkspace" });
      const preview = wrapper.findComponent({ name: "PythonViewPreview" });
      return { tabbar, workspace, preview };
    };

    it("renders only the workspace if no preview is available", async () => {
      const { wrapper } = await doMount({ viewAvailable: false });
      const { tabbar, workspace, preview } = findComponents(wrapper);
      expect(tabbar.exists()).toBeFalsy();
      expect(workspace.exists()).toBeTruthy();
      expect(preview.exists()).toBeFalsy();
    });

    it("renders workspace and preview if preview is available", async () => {
      const { wrapper } = await doMount({ viewAvailable: true });
      const { tabbar, workspace, preview } = findComponents(wrapper);
      expect(tabbar.exists()).toBeTruthy();
      expect(workspace.exists()).toBeTruthy();
      expect(preview.exists()).toBeTruthy();
    });

    it("shows workspace if active tab", async () => {
      const { wrapper } = await doMount({ viewAvailable: true });
      const { tabbar, workspace, preview } = findComponents(wrapper);

      // Select the workspace tab
      tabbar.vm.$emit("update:modelValue", "workspace");
      await flushPromises();

      expect(workspace.isVisible()).toBeTruthy();
      expect(preview.isVisible()).toBeFalsy();
    });

    it("shows preview if active tab", async () => {
      const { wrapper } = await doMount({ viewAvailable: true });
      const { tabbar, workspace, preview } = findComponents(wrapper);

      // Select the preview tab
      tabbar.vm.$emit("update:modelValue", "preview");
      await flushPromises();

      expect(workspace.isVisible()).toBeFalsy();
      expect(preview.isVisible()).toBeTruthy();
    });
  });

  it("saves settings", async () => {
    const { wrapper } = await doMount();
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
    await doMount();
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      [settingsMock.executableSelection],
    );
    const executableSelectionStore = useExecutableSelectionStore();
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
    await doMount();
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
    await doMount({
      executableOptions: [
        executableOptionsMock[0],
        {
          type: "MISSING_VAR",
          id: "conda.environment1",
        },
      ],
    });
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      [settingsMock.executableSelection],
    );
    expect(getScriptingService().sendToConsole).toHaveBeenCalled();
  });
});
