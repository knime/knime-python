import { setSelectedExecutable, useExecutableSelectionStore } from "@/store";
import {
  ScriptingEditor,
  getScriptingService,
  editor,
} from "@knime/scripting-editor";
import {
  VueWrapper,
  enableAutoUnmount,
  flushPromises,
  mount,
} from "@vue/test-utils";
import { afterEach, describe, expect, it, vi } from "vitest";
import { executableOptionsMock } from "../../__mocks__/executable-options";
import App from "../App.vue";
import { nextTick } from "vue";
import { pythonScriptingService } from "@/python-scripting-service";

describe("App.vue", () => {
  enableAutoUnmount(afterEach);

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
        } else if (methodName === "sendLastConsoleOutput") {
          // do nothing
          return Promise.resolve();
        } else if (methodName === "getLanguageServerConfig") {
          return JSON.stringify({ test: "" });
        } else if (methodName === "updateExecutableSelection") {
          // do nothing
          return Promise.resolve();
        }
        throw Error(`Method ${methodName} was not mocked but called in test`);
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
    await flushPromises();
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
      const tabbar = wrapper.findComponent({ ref: "rightTabBar" });
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
      await nextTick();

      expect(workspace.isVisible()).toBeTruthy();
      expect(preview.isVisible()).toBeFalsy();
    });

    it("shows preview if active tab", async () => {
      const { wrapper } = await doMount({ viewAvailable: true });
      const { tabbar, workspace, preview } = findComponents(wrapper);

      // Select the preview tab
      tabbar.vm.$emit("update:modelValue", "preview");
      await nextTick();

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

  it("should update editor model options", async () => {
    await doMount();
    const mainEditorStore = editor.useMainCodeEditorStore();
    expect(
      mainEditorStore.value?.editorModel.updateOptions,
    ).toHaveBeenCalledWith({
      tabSize: 4,
      insertSpaces: true,
    });
  });

  it("should replace tabs on paste", async () => {
    await doMount();
    const mainEditorStore = editor.useMainCodeEditorStore();
    expect(
      mainEditorStore.value?.editor.value?.onDidPaste,
    ).toHaveBeenCalledOnce();

    // Mock the action function
    const actionFn = vi.fn();
    const getActionFn = vi.fn(() => ({
      run: actionFn,
    }));
    mainEditorStore.value!.editor.value!.getAction = getActionFn as any;

    const onDidChangeFn = vi.mocked(mainEditorStore.value!.editor.value!)
      .onDidPaste.mock.calls[0][0];
    onDidChangeFn({} as any);

    expect(getActionFn).toHaveBeenCalledOnce();
    expect(getActionFn).toHaveBeenCalledWith(
      "editor.action.indentationToSpaces",
    );
    expect(actionFn).toHaveBeenCalledOnce();
    expect(actionFn).toHaveBeenCalledWith();
  });

  it("should connect editor to language server", async () => {
    const connectToLanguageServerSpy = vi.spyOn(
      pythonScriptingService,
      "connectToLanguageServer",
    );
    await doMount();
    expect(connectToLanguageServerSpy).toHaveBeenCalledOnce();
    expect(connectToLanguageServerSpy).toHaveBeenCalledWith();
  });

  it("should register input completions", async () => {
    const registerInputCompletionsSpy = vi.spyOn(
      pythonScriptingService,
      "registerInputCompletions",
    );
    await doMount();
    expect(registerInputCompletionsSpy).toHaveBeenCalledOnce();
    expect(registerInputCompletionsSpy).toHaveBeenCalledWith();
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
      "updateExecutableSelection",
      [settingsMock.executableSelection],
    );
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      [settingsMock.executableSelection],
    );
    const executableSelectionStore = useExecutableSelectionStore();
    expect(executableSelectionStore.id).toBe(settingsMock.executableSelection);
    expect(executableSelectionStore.isMissing).toBe(false);
    expect(getScriptingService().sendToConsole).not.toHaveBeenCalled();
  });

  it("sends output of last execution on mount", async () => {
    await doMount();
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "sendLastConsoleOutput",
    );
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
