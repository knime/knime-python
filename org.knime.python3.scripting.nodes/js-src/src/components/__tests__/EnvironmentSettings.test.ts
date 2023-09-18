import { describe, it, beforeEach, afterEach, vi, expect } from "vitest";
import EnvironmentSettings from "../EnvironmentSettings.vue";
import { flushPromises, mount } from "@vue/test-utils";
import { setSelectedExecutable } from "@/store";
import { getScriptingService } from "@knime/scripting-editor";
import { executableOptionsMock } from "@/__mocks__/executable-options";
import Dropdown from "webapps-common/ui/components/forms/Dropdown.vue";
import type { KillSessionInfo, StartSessionInfo } from "@/types/common";

describe("EnvironmentSettings", () => {
  const executableOptions = executableOptionsMock;

  beforeEach(() => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(
      vi.fn((methodName: string): any => {
        if (methodName === "getExecutableOptionsList") {
          return executableOptions;
        }
        if (methodName === "killSession") {
          return {
            status: "SUCCESS",
            description: "",
          } as KillSessionInfo;
        }
        if (methodName === "startInteractive") {
          return {
            status: "SUCCESS",
            description: "",
          } as StartSessionInfo;
        }
        throw Error(`Method ${methodName} was not mocked but called in test`);
      }),
    );
    setSelectedExecutable({ id: "", isMissing: false });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("loads executable options on mount", async () => {
    mount(EnvironmentSettings);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      expect.anything(),
    );
  });

  it("does not display executable options if default is selected", async () => {
    const wrapper = mount(EnvironmentSettings);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      expect.anything(),
    );
    (wrapper.vm as any).selectedEnv = "default";
    await wrapper.vm.$nextTick();
    expect(wrapper.findComponent(Dropdown).exists()).toBeFalsy();
  });

  it("displays executable options if conda is selected", async () => {
    const wrapper = mount(EnvironmentSettings);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "getExecutableOptionsList",
      expect.anything(),
    );
    (wrapper.vm as any).selectedEnv = "conda";
    await wrapper.vm.$nextTick();
    expect(wrapper.findComponent(Dropdown).exists()).toBeTruthy();
  });

  it("restarts python session if executable was changed", async () => {
    const wrapper = mount(EnvironmentSettings);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
      1,
      "getExecutableOptionsList",
      expect.anything(),
    );
    (wrapper.vm as any).selectedEnv = "conda";
    await wrapper.vm.$nextTick();
    (wrapper.vm as any).selectedExecutableOption = "conda.environment1";
    await wrapper.vm.$nextTick();
    wrapper.unmount();
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
      2,
      "killSession",
    );
    expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
      3,
      "startInteractive",
      ["conda.environment1"],
    );
    expect(getScriptingService().sendToConsole).toHaveBeenCalled();
  });

  it("does not restart session if executable didn't change", async () => {
    const wrapper = mount(EnvironmentSettings);
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenNthCalledWith(
      1,
      "getExecutableOptionsList",
      expect.anything(),
    );
    wrapper.unmount();
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledOnce();
    expect(getScriptingService().sendToConsole).not.toHaveBeenCalled();
  });

  describe("error handling", () => {
    it("displays error if no executable options are available", async () => {
      // only default executable
      getScriptingService().sendToService = vi.fn((): any => [
        executableOptionsMock[0],
      ]);
      const wrapper = mount(EnvironmentSettings);
      await flushPromises();
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "getExecutableOptionsList",
        [""],
      );
      (wrapper.vm as any).selectedEnv = "conda";
      await wrapper.vm.$nextTick();
      const errorText = wrapper.find(".error-text");
      expect(errorText.exists()).toBeTruthy();
      expect(errorText.text()).toContain(
        "Connect Conda Environment Propagation Node to Python Script Node",
      );
    });

    it("displays error if missing executable is selected", async () => {
      // missing executable
      getScriptingService().sendToService = vi.fn((): any => [
        executableOptionsMock[0],
        {
          type: "MISSING_VAR",
          id: "conda.environment1",
        },
      ]);
      const wrapper = mount(EnvironmentSettings);
      await flushPromises();
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "getExecutableOptionsList",
        [""],
      );
      (wrapper.vm as any).selectedEnv = "conda";
      await wrapper.vm.$nextTick();
      const errorText = wrapper.find(".error-text");
      expect(errorText.exists()).toBeTruthy();
      expect(errorText.text()).toContain(
        "The currently selected flow variable is not available",
      );
    });

    it("does not restart session if selected executable is missing", async () => {
      // missing executable
      getScriptingService().sendToService = vi.fn((): any => [
        executableOptionsMock[0],
        {
          type: "MISSING_VAR",
          id: "conda.environment1",
        },
      ]);
      const wrapper = mount(EnvironmentSettings);
      await flushPromises();
      expect(getScriptingService().sendToService).toHaveBeenCalledWith(
        "getExecutableOptionsList",
        [""],
      );
      (wrapper.vm as any).selectedEnv = "conda";
      await wrapper.vm.$nextTick();
      wrapper.unmount();
      await flushPromises();
      expect(getScriptingService().sendToService).toHaveBeenCalledOnce();
      expect(getScriptingService().sendToService).not.toHaveBeenCalledWith(
        "killSession",
      );
    });
  });
});
