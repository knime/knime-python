import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { executableOptionsMock } from "@/__mocks__/executable-options";
import { setSelectedExecutable } from "@/store";
import { getScriptingService, consoleHandler } from "@knime/scripting-editor";
import { flushPromises, mount } from "@vue/test-utils";
import { Dropdown } from "@knime/components";
import EnvironmentSettings from "../EnvironmentSettings.vue";
import { getPythonInitialDataService } from "@/python-initial-data-service";
import { DEFAULT_INITIAL_DATA } from "@/__mocks__/mock-data";
import type { ExecutableOption } from "@/types/common";

describe("EnvironmentSettings", () => {
  const executableOptions = executableOptionsMock;

  const doMount = async () => {
    const wrapper = mount(EnvironmentSettings);
    await flushPromises();
    return wrapper;
  };

  beforeEach(() => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(
      vi.fn((methodName: string): any => {
        if (methodName === "updateExecutableSelection") {
          return Promise.resolve();
        }
        throw Error(`Method ${methodName} was not mocked but called in test`);
      }),
    );

    vi.mocked(getPythonInitialDataService).mockReturnValue({
      getInitialData: () =>
        Promise.resolve({
          ...DEFAULT_INITIAL_DATA,
          executableOptionsList: executableOptions as ExecutableOption[],
        }),
    });

    setSelectedExecutable({ id: "", isMissing: false });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("loads initial data on mount", async () => {
    await doMount();

    expect(getPythonInitialDataService).toHaveBeenCalled();
  });

  it("does not display executable options if default is selected", async () => {
    const wrapper = await doMount();

    (wrapper.vm as any).selectedEnv = "default";
    await wrapper.vm.$nextTick();
    expect(wrapper.findComponent(Dropdown).exists()).toBeFalsy();
  });

  it("displays executable options if conda is selected", async () => {
    const wrapper = await doMount();

    (wrapper.vm as any).selectedEnv = "conda";
    await wrapper.vm.$nextTick();
    expect(wrapper.findComponent(Dropdown).exists()).toBeTruthy();
  });

  it("notifies backend if executable was changed", async () => {
    const consoleSpy = vi.spyOn(consoleHandler, "writeln");
    const wrapper = await doMount();

    (wrapper.vm as any).selectedEnv = "conda";
    await wrapper.vm.$nextTick();
    (wrapper.vm as any).selectedExecutableOption = "conda.environment1";
    await wrapper.vm.$nextTick();
    wrapper.unmount();
    await flushPromises();
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "updateExecutableSelection",
      ["conda.environment1"],
    );
    expect(consoleSpy).toHaveBeenCalledWith({
      text: "Changed python executable to /opt/homebrew/Caskroom/miniconda/base/envs/conda_environment1/bin/python",
    });
  });

  it("does not restart session if executable didn't change", async () => {
    const consoleSpy = vi.spyOn(consoleHandler, "writeln");

    const wrapper = await doMount();

    wrapper.unmount();
    await flushPromises();

    expect(consoleSpy).not.toHaveBeenCalled();
  });

  describe("error handling", () => {
    it("displays error if no executable options are available", async () => {
      // only default executable
      vi.mocked(getPythonInitialDataService).mockReturnValue({
        getInitialData: () =>
          Promise.resolve({
            ...DEFAULT_INITIAL_DATA,
            executableOptionsList: [],
          }),
      });

      const wrapper = await doMount();

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
      vi.mocked(getPythonInitialDataService).mockReturnValue({
        getInitialData: () =>
          Promise.resolve({
            ...DEFAULT_INITIAL_DATA,
            executableOptionsList: [
              executableOptionsMock[0],
              {
                type: "MISSING_VAR",
                id: "conda.environment1",
              },
            ] as ExecutableOption[],
          }),
      });

      const wrapper = await doMount();

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
      vi.mocked(getPythonInitialDataService).mockReturnValue({
        getInitialData: () =>
          Promise.resolve({
            ...DEFAULT_INITIAL_DATA,
            executableOptionsList: [
              executableOptionsMock[0],
              {
                type: "MISSING_VAR",
                id: "conda.environment1",
              },
            ] as ExecutableOption[],
          }),
      });

      const wrapper = await doMount();

      (wrapper.vm as any).selectedEnv = "conda";
      await wrapper.vm.$nextTick();

      wrapper.unmount();
      await flushPromises();

      expect(getScriptingService().sendToService).not.toHaveBeenCalledWith(
        "killSession",
      );
    });
  });
});
